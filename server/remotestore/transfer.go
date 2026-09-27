package remotestore

import (
	"context"
	"io"
	"strings"
	"time"

	"emperror.dev/errors"
)

// 支持的远端存储协议。面板 config('external-backups.drivers') 需要与这里保持一致。
const (
	TypeSFTP   = "sftp"
	TypeFTP    = "ftp"
	TypeWebDAV = "webdav"
)

// Storage 是面板下发的一份远端存储配置。凭据只在任务执行期存在于内存中，
// 不落盘、不写日志。
type Storage struct {
	Type        string `json:"type"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Endpoint    string `json:"endpoint"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Token       string `json:"token"`
	PrivateKey  string `json:"private_key"`
	Fingerprint string `json:"fingerprint"`
	RemotePath  string `json:"remote_path"`
	TLS         bool   `json:"tls"`
	Passive     bool   `json:"passive"`
}

// Validate 只做必填项检查，连通性由实际拨号去证明。
func (s *Storage) Validate() error {
	if s.Username == "" {
		return errors.New("transfer: username is required")
	}

	switch s.Type {
	case TypeWebDAV:
		if !strings.HasPrefix(s.Endpoint, "http://") && !strings.HasPrefix(s.Endpoint, "https://") {
			return errors.New("transfer: webdav endpoint must be an http(s) URL")
		}
	case TypeSFTP, TypeFTP:
		if s.Host == "" {
			return errors.New("transfer: host is required")
		}
		if s.Password == "" && s.PrivateKey == "" {
			return errors.New("transfer: either a password or a private key is required")
		}
	default:
		return errors.New("transfer: unsupported storage type " + s.Type)
	}

	return nil
}

// JoinRemotePath 把用户填写的远端目录与文件名拼成完整远端路径。
func (s *Storage) JoinRemotePath(name string) string {
	base := strings.Trim(s.RemotePath, "/")
	if base == "" {
		return strings.TrimPrefix(name, "/")
	}

	return base + "/" + strings.TrimPrefix(name, "/")
}

// Object is a single remote entry below a directory.
type Object struct {
	Path    string
	Size    int64
	ModTime time.Time
}

// Uploader is implemented by every protocol adapter. Upload is the only
// operation that has to stream; the rest exist so the daemon can prune old
// archives the way the panel asks it to.
type Uploader interface {
	Type() string
	Upload(ctx context.Context, remotePath string, r io.Reader, size int64) error
	List(ctx context.Context, dir string) ([]Object, error)
	Delete(ctx context.Context, remotePath string) error
	Close() error
}

// New builds the adapter matching the storage type.
func New(ctx context.Context, s *Storage) (Uploader, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	switch s.Type {
	case TypeSFTP:
		return newSFTP(ctx, s)
	case TypeFTP:
		return newFTP(ctx, s)
	case TypeWebDAV:
		return newWebDAV(s)
	default:
		return nil, errors.New("transfer: unsupported storage type " + s.Type)
	}
}
