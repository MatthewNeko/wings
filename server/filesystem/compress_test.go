package filesystem

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	. "github.com/franela/goblin"
	"golang.org/x/text/encoding/simplifiedchinese"
)

// Given an archive named test.{ext}, with the following file structure:
//
//	test/
//	|──inside/
//	|────finside.txt
//	|──outside.txt
//
// this test will ensure that it's being decompressed as expected
func TestFilesystem_DecompressFile(t *testing.T) {
	g := Goblin(t)
	fs, rfs := NewFs()

	g.Describe("Decompress", func() {
		for _, ext := range []string{"zip", "rar", "tar", "tar.gz"} {
			g.It("can decompress a "+ext, func() {
				// copy the file to the new FS
				c, err := os.ReadFile("./testdata/test." + ext)
				g.Assert(err).IsNil()
				err = rfs.CreateServerFile("./test."+ext, c)
				g.Assert(err).IsNil()

				// decompress
				err = fs.DecompressFile(context.Background(), "/", "test."+ext)
				g.Assert(err).IsNil()

				// make sure everything is where it is supposed to be
				_, err = rfs.StatServerFile("test/outside.txt")
				g.Assert(err).IsNil()

				st, err := rfs.StatServerFile("test/inside")
				g.Assert(err).IsNil()
				g.Assert(st.IsDir()).IsTrue()

				_, err = rfs.StatServerFile("test/inside/finside.txt")
				g.Assert(err).IsNil()
				g.Assert(st.IsDir()).IsTrue()
			})
		}

		g.AfterEach(func() {
			_ = fs.TruncateRootDirectory()
		})
	})
}

// TestFilesystem_DecompressFileWithProgress covers the progress plumbing used by
// the batch decompression endpoint: the size estimate taken from the archive
// itself, the bytes reported while extracting, and cancellation.
func TestFilesystem_DecompressFileWithProgress(t *testing.T) {
	g := Goblin(t)
	fs, rfs := NewFs()

	g.Describe("Decompress with progress", func() {
		g.It("reports the size of the archive contents up front", func() {
			c, err := zipWithFile("data/one.txt", 4096)
			g.Assert(err).IsNil()
			g.Assert(rfs.CreateServerFile("./size.zip", c)).IsNil()

			total, err := fs.UncompressedSize(context.Background(), "/", "size.zip")
			g.Assert(err).IsNil()
			g.Assert(total).Equal(int64(4096))
		})

		g.It("reports every byte it writes while extracting", func() {
			c, err := zipWithFile("data/two.txt", 8192)
			g.Assert(err).IsNil()
			g.Assert(rfs.CreateServerFile("./counted.zip", c)).IsNil()

			var written int64
			err = fs.DecompressFileWithProgress(context.Background(), "/", "counted.zip", func(n int64) {
				atomic.AddInt64(&written, n)
			}, nil)
			g.Assert(err).IsNil()
			g.Assert(atomic.LoadInt64(&written)).Equal(int64(8192))

			_, err = rfs.StatServerFile("data/two.txt")
			g.Assert(err).IsNil()
		})

		g.It("stops extracting once cancellation is requested", func() {
			c, err := zipWithFile("data/three.txt", 1024)
			g.Assert(err).IsNil()
			g.Assert(rfs.CreateServerFile("./cancelled.zip", c)).IsNil()

			cancel := make(chan struct{})
			close(cancel)

			err = fs.DecompressFileWithProgress(context.Background(), "/", "cancelled.zip", nil, cancel)
			g.Assert(err).IsNotNil()
			g.Assert(strings.Contains(err.Error(), "context canceled")).IsTrue()

			// Nothing should have made it to disk.
			_, err = rfs.StatServerFile("data/three.txt")
			g.Assert(err).IsNotNil()
		})

		g.It("rejects files that are not archives", func() {
			g.Assert(rfs.CreateServerFile("./plain.txt", []byte("not an archive"))).IsNil()

			_, err := fs.UncompressedSize(context.Background(), "/", "plain.txt")
			g.Assert(IsErrorCode(err, ErrCodeUnknownArchive)).IsTrue()
		})

		g.AfterEach(func() {
			_ = fs.TruncateRootDirectory()
		})
	})
}

// zipWithFile builds a zip holding a single file of the requested size, so that
// extraction has real bytes to report on.
func zipWithFile(name string, size int) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	w, err := zw.Create(name)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(bytes.Repeat([]byte("a"), size)); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// TestFilesystem_CompressFilesWithProgress covers the plumbing behind the
// background compression endpoint: the upfront size estimate of the source tree,
// the byte callbacks fired while packing, and the cleanup that has to happen when
// a compression is cancelled halfway through.
func TestFilesystem_CompressFilesWithProgress(t *testing.T) {
	g := Goblin(t)
	fs, rfs := NewFs()

	g.Describe("Compress with progress", func() {
		// Archives are written next to the sources they came from, so every case
		// has to start from a clean tree to tell them apart.
		g.AfterEach(func() {
			_ = fs.TruncateRootDirectory()
		})

		g.It("estimates the size of the source tree up front", func() {
			g.Assert(rfs.CreateServerFile("./one.txt", bytes.Repeat([]byte("a"), 1024))).IsNil()
			g.Assert(mkdirAll(fs.Path(), "nested/deep")).IsNil()
			g.Assert(rfs.CreateServerFile("./nested/deep/two.txt", bytes.Repeat([]byte("b"), 2048))).IsNil()
			// Requested explicitly, but unreadable, so it must not inflate the total.
			g.Assert(rfs.CreateServerFile("./three.txt", bytes.Repeat([]byte("c"), 4096))).IsNil()

			total := fs.CompressSourceSize(context.Background(), "/", []string{"one.txt", "nested"})
			g.Assert(total).Equal(int64(3072))

			// Naming every file behaves the same as naming a directory.
			g.Assert(fs.CompressSourceSize(context.Background(), "/", []string{"one.txt", "nested/deep/two.txt"})).Equal(int64(3072))
		})

		g.It("reports the source bytes consumed while packing", func() {
			g.Assert(rfs.CreateServerFile("./one.txt", bytes.Repeat([]byte("a"), 4096))).IsNil()
			g.Assert(mkdirAll(fs.Path(), "nested")).IsNil()
			g.Assert(rfs.CreateServerFile("./nested/two.txt", bytes.Repeat([]byte("b"), 8192))).IsNil()

			var written int64
			info, err := fs.CompressFilesWithProgress(context.Background(), "/", []string{"one.txt", "nested"}, func(n int64) {
				atomic.AddInt64(&written, n)
			})
			g.Assert(err).IsNil()
			g.Assert(info).IsNotNil()

			// The callback counts the uncompressed side, so it should line up with
			// what went in rather than the (compressed) size of the archive.
			g.Assert(atomic.LoadInt64(&written)).Equal(int64(12288))

			names, err := tarEntries(fs, info.Name())
			g.Assert(err).IsNil()
			// Directory walk order is not part of the contract, so compare a sorted
			// copy and simply assert that both requested paths made it in.
			slices.Sort(names)
			g.Assert(names).Equal([]string{"nested/two.txt", "one.txt"})
		})

		g.It("throws away a half written archive when cancelled", func() {
			g.Assert(rfs.CreateServerFile("./big.txt", bytes.Repeat([]byte("a"), 4<<20))).IsNil()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Cancel as soon as the first chunk lands, which is the mid-flight case
			// that a truncated tar.gz would otherwise be left behind for.
			info, err := fs.CompressFilesWithProgress(ctx, "/", []string{"big.txt"}, func(int64) {
				cancel()
			})
			g.Assert(err).IsNotNil()
			g.Assert(info).IsNil()

			g.Assert(archiveNames(fs)).Equal([]string{})
		})

		g.It("does not leave an archive behind when the context is already done", func() {
			g.Assert(rfs.CreateServerFile("./one.txt", bytes.Repeat([]byte("a"), 1024))).IsNil()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err := fs.CompressFilesWithProgress(ctx, "/", []string{"one.txt"}, nil)
			g.Assert(err).IsNotNil()
			g.Assert(archiveNames(fs)).Equal([]string{})
		})
	})
}

// mkdirAll creates a directory tree relative to the server root, since the test
// helpers only know how to write files.
func mkdirAll(root string, p string) error {
	return os.MkdirAll(filepath.Join(root, p), 0o777)
}

// tarEntries reads the named archive from the server root and returns the paths of
// the regular files it holds.
func tarEntries(fs *Filesystem, name string) ([]string, error) {
	f, _, err := fs.File(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	out := []string{}
	tr := tar.NewReader(gr)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		if h.Typeflag == tar.TypeReg {
			out = append(out, h.Name)
		}
	}

	return out, nil
}

// archiveNames lists the compression output files currently sitting in the root of
// the server directory.
func archiveNames(fs *Filesystem) []string {
	entries, err := os.ReadDir(fs.Path())
	if err != nil {
		return []string{err.Error()}
	}

	out := []string{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "archive-") {
			out = append(out, e.Name())
		}
	}

	return out
}

// Test for GBK-encoded filenames in archives
func TestFilesystem_DecompressFile_GBK(t *testing.T) {
	g := Goblin(t)
	fs, rfs := NewFs()

	g.Describe("Decompress GBK-encoded filenames", func() {
		g.It("can decompress a zip with GBK-encoded filenames", func() {
			// copy the file to the new FS
			c, err := os.ReadFile("./testdata/test-gbk.zip")
			g.Assert(err).IsNil()
			err = rfs.CreateServerFile("./test-gbk.zip", c)
			g.Assert(err).IsNil()

			// decompress
			err = fs.DecompressFile(context.Background(), "/", "test-gbk.zip")
			g.Assert(err).IsNil()

			// make sure the file was extracted with proper UTF-8 filename
			_, err = rfs.StatServerFile("测试文件夹/测试文档.txt")
			g.Assert(err).IsNil()
		})

		g.It("can check space for a zip with GBK-encoded filenames", func() {
			// copy the file to the new FS
			c, err := os.ReadFile("./testdata/test-gbk.zip")
			g.Assert(err).IsNil()
			err = rfs.CreateServerFile("./test-gbk.zip", c)
			g.Assert(err).IsNil()

			// check space availability
			err = fs.SpaceAvailableForDecompression(context.Background(), "/", "test-gbk.zip")
			g.Assert(err).IsNil()
		})

		g.AfterEach(func() {
			_ = fs.TruncateRootDirectory()
		})
	})
}

// Test the decodeFilename helper function
func TestDecodeFilename(t *testing.T) {
	g := Goblin(t)

	g.Describe("decodeFilename", func() {
		g.It("should pass through valid UTF-8 strings", func() {
			input := "test/测试文件.txt"
			output := decodeFilename(input)
			g.Assert(output).Equal(input)
		})

		g.It("should pass through ASCII strings", func() {
			input := "test/file.txt"
			output := decodeFilename(input)
			g.Assert(output).Equal(input)
		})

		g.It("should convert GBK to UTF-8", func() {
			// Create a GBK-encoded string
			utf8String := "测试文件.txt"
			gbkString, err := simplifiedchinese.GBK.NewEncoder().String(utf8String)
			g.Assert(err).IsNil()

			// Verify it's not valid UTF-8
			g.Assert(utf8.ValidString(gbkString)).IsFalse()

			// Decode and verify it matches original UTF-8
			output := decodeFilename(gbkString)
			g.Assert(output).Equal(utf8String)
		})
	})
}

// Empty directories have no file to create them implicitly, so extraction must
// create them explicitly or they are dropped.
func TestFilesystem_DecompressFileEmptyDirectory(t *testing.T) {
	g := Goblin(t)
	fs, rfs := NewFs()

	g.Describe("Decompress", func() {
		archives := []struct {
			name  string
			build func() ([]byte, error)
		}{
			{"empty.zip", zipWithEmptyDir},
			{"empty.tar.gz", tarGzWithEmptyDir},
		}

		for _, a := range archives {
			g.It("preserves an empty directory in a "+a.name, func() {
				content, err := a.build()
				g.Assert(err).IsNil()
				err = rfs.CreateServerFile("./"+a.name, content)
				g.Assert(err).IsNil()

				err = fs.DecompressFile(context.Background(), "/", a.name)
				g.Assert(err).IsNil()

				// The empty directory must exist, and the sibling file must still extract.
				st, err := rfs.StatServerFile("empty")
				g.Assert(err).IsNil()
				g.Assert(st.IsDir()).IsTrue()

				_, err = rfs.StatServerFile("outside.txt")
				g.Assert(err).IsNil()
			})
		}

		g.AfterEach(func() {
			_ = fs.TruncateRootDirectory()
		})
	})
}

// zipWithEmptyDir builds a zip holding one file and an empty directory ("empty/").
func zipWithEmptyDir() ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	dh := &zip.FileHeader{Name: "empty/"}
	dh.SetMode(os.ModeDir | 0o755)
	if _, err := zw.CreateHeader(dh); err != nil {
		return nil, err
	}

	w, err := zw.Create("outside.txt")
	if err != nil {
		return nil, err
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// tarGzWithEmptyDir builds a tar.gz holding one file and an empty directory ("empty/").
func tarGzWithEmptyDir() ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	if err := tw.WriteHeader(&tar.Header{Name: "empty/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		return nil, err
	}

	content := []byte("hello")
	if err := tw.WriteHeader(&tar.Header{Name: "outside.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(content); err != nil {
		return nil, err
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
