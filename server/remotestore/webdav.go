package remotestore

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"emperror.dev/errors"
)

// webdavUploader 只依赖标准库实现 WebDAV 的 PUT/MKCOL/PROPFIND/DELETE，
// 这样坚果云、Nextcloud、ownCloud 这类网盘都能直接用，也方便把限速
// 直接套在请求体上。
type webdavUploader struct {
	client   *http.Client
	base     string
	username string
	password string
	token    string
}

func newWebDAV(s *Storage) (Uploader, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 60 * time.Second

	return &webdavUploader{
		// 不设 client.Timeout：一次上传可能持续数小时，整体超时交给 context 控制。
		client:   &http.Client{Transport: transport},
		base:     strings.TrimRight(s.Endpoint, "/"),
		username: s.Username,
		password: s.Password,
		token:    s.Token,
	}, nil
}

func (u *webdavUploader) Type() string { return TypeWebDAV }

func (u *webdavUploader) Upload(ctx context.Context, remotePath string, r io.Reader, size int64) error {
	if err := u.mkCol(ctx, path.Dir(remotePath)); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.urlFor(remotePath), r)
	if err != nil {
		return errors.WrapIf(err, "transfer: webdav request failed")
	}

	// 给出长度让服务端可以直接落文件；长度未知时退回分块传输。
	if size > 0 {
		req.ContentLength = size
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	u.authorize(req)

	resp, err := u.client.Do(req)
	if err != nil {
		return errors.WrapIf(err, "transfer: webdav upload failed")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return errors.Errorf("transfer: webdav upload rejected with status %d (%s)", resp.StatusCode, readSnip(resp.Body))
	}

	return nil
}

// mkCol 逐级创建目录。目录已存在时服务端返回 405，属于正常情况。
func (u *webdavUploader) mkCol(ctx context.Context, dir string) error {
	dir = strings.Trim(dir, "/")
	if dir == "" || dir == "." {
		return nil
	}

	current := ""
	for _, segment := range strings.Split(dir, "/") {
		if segment == "" {
			continue
		}

		current += "/" + segment

		req, err := http.NewRequestWithContext(ctx, "MKCOL", u.urlFor(strings.TrimPrefix(current, "/")), nil)
		if err != nil {
			return errors.WrapIf(err, "transfer: webdav mkdir request failed")
		}
		u.authorize(req)

		resp, err := u.client.Do(req)
		if err != nil {
			return errors.WrapIf(err, "transfer: webdav mkdir failed")
		}

		created := resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent

		_ = resp.Body.Close()

		// 405/409 分别表示目录已存在与父目录还没有，父目录的情况会在下一轮
		// 或者真正 PUT 时暴露出来，这里不当成错误中断流程。
		if !created && resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusConflict {
			return errors.Errorf("transfer: webdav mkdir returned unexpected status %d for %s", resp.StatusCode, current)
		}
	}

	return nil
}

type propResult struct {
	GetContentLength string `xml:"getcontentlength"`
	GetLastModified  string `xml:"getlastmodified"`
	ResourceType     struct {
		Collection string `xml:"collection"`
	} `xml:"resourcetype"`
}

type propStat struct {
	Prop propResult `xml:"prop"`
}

type davResponse struct {
	Href      string     `xml:"href"`
	PropStats []propStat `xml:"propstat"`
}

type multistatus struct {
	Responses []davResponse `xml:"response"`
}

func (u *webdavUploader) List(ctx context.Context, dir string) ([]Object, error) {
	target := u.urlFor(dir)

	req, err := http.NewRequestWithContext(ctx, "PROPFIND", target, nil)
	if err != nil {
		return nil, errors.WrapIf(err, "transfer: webdav list request failed")
	}

	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	req.Body = io.NopCloser(bytes.NewBufferString(`<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:resourcetype/><d:getcontentlength/><d:getlastmodified/></d:prop></d:propfind>`))
	u.authorize(req)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, errors.WrapIf(err, "transfer: webdav list failed")
	}
	defer resp.Body.Close()

	// 部分网盘对空目录返回 404，这里按“没有文件”处理，让清理逻辑继续走。
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("transfer: webdav list returned status %d", resp.StatusCode)
	}

	var status multistatus
	if err := xml.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, errors.WrapIf(err, "transfer: webdav list response is not valid XML")
	}

	prefix := strings.TrimRight(dir, "/")
	objects := make([]Object, 0, len(status.Responses))

	for _, r := range status.Responses {
		href, err := url.PathUnescape(r.Href)
		if err != nil {
			continue
		}

		name := strings.TrimRight(href, "/")
		if name == "" || strings.HasSuffix(href, "/") {
			continue
		}

		if !strings.HasSuffix(name, prefix) {
			// href 是从站点根算起的完整路径，只保留本次目录下的条目。
			if !strings.Contains(name, prefix+"/") {
				continue
			}
		}

		var size int64
		var isDir bool
		var modified time.Time

		for _, ps := range r.PropStats {
			if ps.Prop.ResourceType.Collection != "" {
				isDir = true
			}

			if v, err := strconv.ParseInt(ps.Prop.GetContentLength, 10, 64); err == nil {
				size = v
			}

			if v, err := http.ParseTime(ps.Prop.GetLastModified); err == nil {
				modified = v
			}
		}

		if isDir {
			continue
		}

		rel := path.Base(name)
		if idx := strings.Index(href, prefix+"/"); idx >= 0 {
			rel = strings.TrimPrefix(href[idx+len(prefix)+1:], "/")
		}

		rel = strings.TrimSuffix(rel, "/")
		if rel == "" || rel == strings.TrimPrefix(prefix, "/") || strings.HasSuffix(r.Href, "/") {
			continue
		}

		objects = append(objects, Object{Path: strings.TrimPrefix(path.Join(prefix, rel), "//"), Size: size, ModTime: modified})
	}

	return objects, nil
}

func (u *webdavUploader) Delete(ctx context.Context, remotePath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.urlFor(remotePath), nil)
	if err != nil {
		return errors.WrapIf(err, "transfer: webdav delete request failed")
	}
	u.authorize(req)

	resp, err := u.client.Do(req)
	if err != nil {
		return errors.WrapIf(err, "transfer: webdav delete failed")
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return errors.Errorf("transfer: webdav delete returned status %d", resp.StatusCode)
	}

	return nil
}

func (u *webdavUploader) Close() error { return nil }

func (u *webdavUploader) urlFor(p string) string {
	segments := strings.Split(strings.Trim(p, "/"), "/")
	escaped := make([]string, 0, len(segments))

	for _, s := range segments {
		if s == "" {
			continue
		}

		escaped = append(escaped, url.PathEscape(s))
	}

	return u.base + "/" + strings.Join(escaped, "/")
}

func (u *webdavUploader) authorize(req *http.Request) {
	if u.token != "" {
		req.Header.Set("Authorization", "Bearer "+u.token)

		return
	}

	req.SetBasicAuth(u.username, u.password)
}

func readSnip(body io.Reader) string {
	buf := make([]byte, 256)

	n, _ := io.ReadFull(body, buf)
	if n <= 0 {
		return "no body"
	}

	return fmt.Sprintf("%q", strings.TrimSpace(string(buf[:n])))
}
