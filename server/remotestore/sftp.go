package remotestore

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpUploader struct {
	client  *sftp.Client
	conn    *ssh.Client
	timeout time.Duration
}

func newSFTP(ctx context.Context, s *Storage) (Uploader, error) {
	auth, err := sshAuthMethods(s)
	if err != nil {
		return nil, err
	}

	port := s.Port
	if port == 0 {
		port = 22
	}

	cfg := &ssh.ClientConfig{
		User:            s.Username,
		Auth:            auth,
		Timeout:         30 * time.Second,
		HostKeyCallback: hostKeyCallback(s),
	}

	address := net.JoinHostPort(s.Host, fmt.Sprint(port))
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}

	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, errors.WrapIf(err, "transfer: sftp dial failed")
	}

	conn, chans, reqs, err := ssh.NewClientConn(raw, address, cfg)
	if err != nil {
		_ = raw.Close()

		return nil, errors.WrapIf(err, "transfer: sftp handshake failed")
	}

	// ssh.NewClient 只能消费一次 chans/reqs，所以连接对象要建一次、复用两处。
	sshClient := ssh.NewClient(conn, chans, reqs)
	client, err := sftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()

		return nil, errors.WrapIf(err, "transfer: sftp session failed")
	}

	return &sftpUploader{client: client, conn: sshClient, timeout: 30 * time.Second}, nil
}

// sshAuthMethods 支持密码与私钥两种方式。带 passphrase 的私钥需要先解密，
// 这里不引入交互式输入，直接给出明确错误而不是静默失败。
func sshAuthMethods(s *Storage) ([]ssh.AuthMethod, error) {
	methods := make([]ssh.AuthMethod, 0, 2)

	if s.Password != "" {
		methods = append(methods, ssh.Password(s.Password))
	}

	if s.PrivateKey != "" {
		key, err := ssh.ParsePrivateKey([]byte(s.PrivateKey))
		if err != nil {
			if _, ok := err.(*ssh.PassphraseMissingError); ok {
				return nil, errors.New("transfer: 该 SSH 私钥带 passphrase，暂不支持，请改用密码认证")
			}

			return nil, errors.WrapIf(err, "transfer: unable to parse private key")
		}

		methods = append(methods, ssh.PublicKeys(key))
	}

	if len(methods) == 0 {
		return nil, errors.New("transfer: no sftp authentication method available")
	}

	return methods, nil
}

// hostKeyCallback 在用户填了指纹时严格校验，没填时退回不校验并留下一条告警。
// 网盘地址由用户自己提供，无法预置已知主机密钥，这是可用性上的取舍。
func hostKeyCallback(s *Storage) ssh.HostKeyCallback {
	if s.Fingerprint == "" {
		log.WithField("host", s.Host).Warn("sftp 未配置主机密钥指纹，本次连接不校验服务器身份")

		return ssh.InsecureIgnoreHostKey()
	}

	expected := strings.TrimSpace(s.Fingerprint)

	return func(_ string, _ net.Addr, key ssh.PublicKey) error {
		got := ssh.FingerprintSHA256(key)
		if !strings.EqualFold(normalizeFingerprint(got), normalizeFingerprint(expected)) {
			return errors.Errorf("transfer: sftp 主机密钥指纹不匹配，期望 %s，实际 %s", expected, got)
		}

		return nil
	}
}

func normalizeFingerprint(v string) string {
	return strings.ToLower(strings.TrimPrefix(v, "SHA256:"))
}

func (u *sftpUploader) Type() string { return TypeSFTP }

func (u *sftpUploader) Upload(ctx context.Context, remotePath string, r io.Reader, _ int64) error {
	if err := u.ensureContext(ctx); err != nil {
		return err
	}

	if dir := path.Dir(remotePath); dir != "." && dir != "/" {
		if err := u.client.MkdirAll(dir); err != nil {
			return errors.WrapIf(err, "transfer: sftp mkdir failed")
		}
	}

	file, err := u.client.Create(remotePath)
	if err != nil {
		return errors.WrapIf(err, "transfer: sftp create failed")
	}
	defer file.Close()

	// 取消检查靠上层的限速 reader 包裹完成：它每取一块都会看 ctx 是否已结束。
	if _, err := io.Copy(file, r); err != nil {
		return errors.WrapIf(err, "transfer: sftp upload failed")
	}

	return file.Close()
}

func (u *sftpUploader) List(ctx context.Context, dir string) ([]Object, error) {
	if err := u.ensureContext(ctx); err != nil {
		return nil, err
	}

	// pkg/sftp 的 ReadDir 直接返回 FileInfo 列表，不是 fs.DirEntry。
	entries, err := u.client.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(errors.Cause(err)) {
			return nil, nil
		}

		return nil, errors.WrapIf(err, "transfer: sftp list failed")
	}

	objects := make([]Object, 0, len(entries))
	for _, info := range entries {
		if info == nil || info.IsDir() {
			continue
		}

		objects = append(objects, Object{
			Path:    path.Join(dir, info.Name()),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		})
	}

	return objects, nil
}

func (u *sftpUploader) Delete(ctx context.Context, remotePath string) error {
	if err := u.ensureContext(ctx); err != nil {
		return err
	}

	return errors.WrapIf(u.client.Remove(remotePath), "transfer: sftp delete failed")
}

func (u *sftpUploader) Close() error {
	if u.client != nil {
		_ = u.client.Close()
	}

	if u.conn != nil {
		return u.conn.Close()
	}

	return nil
}

func (u *sftpUploader) ensureContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
