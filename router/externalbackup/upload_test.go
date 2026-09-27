package externalbackup

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server/remotestore"
)

// recordingUploader 只做一件事：把流读干净并记账。限速是否真的生效，
// 就体现在读完后花了多少时间、以及读到了多少字节。
type recordingUploader struct {
	remotePath string
	size       int64
	read       int64
}

func (u *recordingUploader) Type() string { return "recording" }

func (u *recordingUploader) Upload(ctx context.Context, remotePath string, r io.Reader, size int64) error {
	u.remotePath = remotePath
	u.size = size

	buf := make([]byte, 64*1024)
	for {
		n, err := r.Read(buf)
		u.read += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}
	}
}

func (u *recordingUploader) List(context.Context, string) ([]remotestore.Object, error) {
	return nil, nil
}

func (u *recordingUploader) Delete(context.Context, string) error { return nil }

func (u *recordingUploader) Close() error { return nil }

func tempArchive(t *testing.T, size int) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "server.tar.gz")
	if err := os.WriteFile(p, make([]byte, size), 0o600); err != nil {
		t.Fatalf("写入临时归档失败: %v", err)
	}

	return p
}

// 用户在面板上填 256 KB/s，Wings 就必须真的把出口掐在这个量级。
func TestUpload_HonoursPanelSpeedLimit(t *testing.T) {
	const kbps = int64(256)
	// 令牌桶起始是满的（容量 = 速率/2 = 128KB），所以 384KB 里只有后 256KB
	// 需要等，理论上花 1 秒；给足余量只断言下限，避免机器抖动导致误报。
	const size = 384 * 1024
	want := time.Duration(float64(size-128*1024) / float64(kbps*1024) * float64(time.Second))
	min := time.Duration(float64(want) * 0.85)

	task := &Task{}
	up := &recordingUploader{}
	src := tempArchive(t, size)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()

	err := task.upload(ctx, up, remote.ExternalBackupInit{
		RemotePath:     "backups/server.tar.gz",
		SpeedLimitKbps: kbps,
	}, src, size)

	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("上传返回错误: %v", err)
	}

	if up.read != size {
		t.Fatalf("远端只读到 %d 字节，应为 %d", up.read, size)
	}

	if up.remotePath != "backups/server.tar.gz" {
		t.Fatalf("目标路径没有原样透传: %s", up.remotePath)
	}

	if task.uploadedBytes != size {
		t.Fatalf("进度计数少了一截: got=%d want=%d", task.uploadedBytes, size)
	}

	if elapsed < min {
		t.Fatalf("限速没有生效：%v 就读完了 %d 字节（期望至少 %v）", elapsed, size, min)
	}

	if elapsed > 15*time.Second {
		t.Fatalf("限速过头了，测试会拖住整个包：%v", elapsed)
	}
}

// 0 表示不限速，这条分支不能因为桶容量为 0 而直接卡死。
func TestUpload_ZeroLimitMeansUnlimited(t *testing.T) {
	const size = 384 * 1024

	task := &Task{}
	up := &recordingUploader{}
	src := tempArchive(t, size)

	start := time.Now()

	if err := task.upload(context.Background(), up, remote.ExternalBackupInit{
		RemotePath:     "server.tar.gz",
		SpeedLimitKbps: 0,
	}, src, size); err != nil {
		t.Fatalf("上传返回错误: %v", err)
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("限速为 0 时不该等这么久: %v", elapsed)
	}

	if up.read != size {
		t.Fatalf("不限速时读全了没有: got=%d want=%d", up.read, size)
	}
}

// 取消窗口要能在读流时被注意到，否则中止一次备份得等整个文件传完。
func TestUpload_StopsWhenContextCancelled(t *testing.T) {
	const size = 384 * 1024

	task := &Task{}
	up := &recordingUploader{}
	src := tempArchive(t, size)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := task.upload(ctx, up, remote.ExternalBackupInit{
		RemotePath:     "server.tar.gz",
		SpeedLimitKbps: 256,
	}, src, size)

	if err == nil {
		t.Fatal("上下文已取消，上传不该报告成功")
	}

	if up.read >= size {
		t.Fatalf("取消后还在继续传：%d/%d", up.read, size)
	}
}

// 三个阶段压进同一根 0-100 的条，归档占大头，回退会让进度条冻住。
func TestPercent_MapsPhasesOntoSingleBar(t *testing.T) {
	cases := []struct {
		name   string
		status string
		source int64
		arch   int64
		up     int64
		want   int
	}{
		{"归档刚开始", StatusArchiving, 1000, 0, 0, 0},
		{"归档一半", StatusArchiving, 1000, 500, 0, 25},
		{"归档估少了也不越界", StatusArchiving, 100, 500, 0, 50},
		{"刚开始上传", StatusUploading, 1000, 1000, 0, 50},
		{"上传一半", StatusUploading, 1000, 1000, 500, 72},
		{"上传超过归档大小", StatusUploading, 1000, 1000, 5000, 95},
		{"清理阶段", StatusPruning, 1000, 1000, 1000, 97},
		{"完成", StatusSuccess, 1000, 1000, 1000, 100},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task := &Task{
				status:        c.status,
				sourceTotal:   c.source,
				archiveBytes:  c.arch,
				uploadedBytes: c.up,
			}

			if got := task.percent(); got != c.want {
				t.Fatalf("进度不对: got=%d want=%d", got, c.want)
			}
		})
	}
}

// 总字节为 0（枚举失败等）时不能算出 NaN，也不能直接显示满格。
func TestPercent_UnknownArchiveSize(t *testing.T) {
	task := &Task{status: StatusArchiving}

	if got := task.percent(); got != 5 {
		t.Fatalf("拿不到总量时应给一个很小的起点: got=%d", got)
	}
}
