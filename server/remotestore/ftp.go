package remotestore

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"path"
	"time"

	"emperror.dev/errors"
	"github.com/jlaffaye/ftp"
)

type ftpUploader struct {
	conn *ftp.ServerConn
}

func newFTP(ctx context.Context, s *Storage) (Uploader, error) {
	port := s.Port
	if port == 0 {
		port = 21
	}

	opts := []ftp.DialOption{
		ftp.DialWithTimeout(30 * time.Second),
		ftp.DialWithShutTimeout(30 * time.Second),
		ftp.DialWithContext(ctx),
	}

	if s.TLS {
		// 显式 TLS（AUTH TLS）是网盘与老 FTP 面板最常见的做法；隐式 TLS 走 990
		// 端口，这里不做自动猜测，用户需要显式选择端口。
		opts = append(opts, ftp.DialWithExplicitTLS(&tls.Config{
			ServerName: s.Host,
			// 用户名与密码本身要加密传输，这里保持默认的证书校验。
			MinVersion: tls.VersionTLS12,
		}))
	}

	conn, err := ftp.Dial(net.JoinHostPort(s.Host, itoa(port)), opts...)
	if err != nil {
		return nil, errors.WrapIf(err, "transfer: ftp dial failed")
	}

	if err := conn.Login(s.Username, s.Password); err != nil {
		_ = conn.Quit()

		return nil, errors.WrapIf(err, "transfer: ftp login failed")
	}

	return &ftpUploader{conn: conn}, nil
}

func (u *ftpUploader) Type() string { return TypeFTP }

func (u *ftpUploader) Upload(ctx context.Context, remotePath string, r io.Reader, _ int64) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}

	if dir := path.Dir(remotePath); dir != "." && dir != "/" {
		// FTP 没有递归建目录的命令，逐级创建；已存在时服务器会报错，忽略即可。
		if err := u.makeDirs(dir); err != nil {
			return err
		}
	}

	// Stor 需要的是二进制流式读取，取消检查由上层限速 reader 在每次 Read 时完成。
	if err := u.conn.Stor(remotePath, r); err != nil {
		return errors.WrapIf(err, "transfer: ftp upload failed")
	}

	return nil
}

func (u *ftpUploader) makeDirs(dir string) error {
	segments := splitPath(dir)
	current := ""

	if isAbs(dir) {
		current = "/"
	}

	for _, segment := range segments {
		if segment == "" {
			continue
		}

		current = path.Join(current, segment)

		if err := u.conn.MakeDir(current); err != nil {
			// 目录已经存在是正常情况，只有真正无法创建时才继续报错，
			// 交给随后的 Stor 暴露出真实原因。
			continue
		}
	}

	return nil
}

func (u *ftpUploader) List(ctx context.Context, dir string) ([]Object, error) {
	if err := ensureContext(ctx); err != nil {
		return nil, err
	}

	entries, err := u.conn.List(dir)
	if err != nil {
		return nil, nil
	}

	objects := make([]Object, 0, len(entries))
	for _, e := range entries {
		if e.Type == ftp.EntryTypeFolder || len(e.Name) == 0 {
			continue
		}

		objects = append(objects, Object{
			Path:    path.Join(dir, e.Name),
			Size:    int64(e.Size),
			ModTime: e.Time,
		})
	}

	return objects, nil
}

func (u *ftpUploader) Delete(ctx context.Context, remotePath string) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}

	return errors.WrapIf(u.conn.Delete(remotePath), "transfer: ftp delete failed")
}

func (u *ftpUploader) Close() error {
	return u.conn.Quit()
}
