package blobs2gzip_test

import (
	"bufio"
	"bytes"
	"iter"
	"testing"

	bg "github.com/takanoriyanagitani/go-blobs2gzip"
)

func TestBlobsToGzip(t *testing.T) {
	t.Parallel()

	t.Run("BlobsToWriter", func(t *testing.T) {
		t.Parallel()

		t.Run("GzipToBlobs", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var gz bytes.Buffer
				var empty iter.Seq2[bg.Blob, error] = func(
					yield func(bg.Blob, error) bool,
				) {
				}

				e := bg.BlobsToWriter(&gz)(bg.Blobs(empty))
				if nil != e {
					t.Fatalf("unexpected error: %v\n", e)
				}

				var blobs iter.Seq2[bg.Blob, error] = bg.GzipToBlobs(
					bufio.NewReader(bytes.NewReader(gz.Bytes())),
				)
				for range blobs {
					t.Fatalf("must be empty\n")
				}
			})

			t.Run("single", func(t *testing.T) {
				t.Parallel()

				var single iter.Seq2[bg.Blob, error] = func(
					yield func(bg.Blob, error) bool,
				) {
					yield([]byte("helo"), nil)
				}

				var gz bytes.Buffer
				e := bg.BlobsToWriter(&gz)(bg.Blobs(single))
				if nil != e {
					t.Fatalf("unexpected error: %v\n", e)
				}

				var blobs iter.Seq2[bg.Blob, error] = bg.GzipToBlobs(
					bufio.NewReader(bytes.NewReader(gz.Bytes())),
				)
				for blob, e := range blobs {
					if nil != e {
						t.Fatalf("unexpected error: %v\n", e)
					}
					var s string = string(blob)
					if s != "helo" {
						t.Fatalf("unexpected value: %v\n", blob)
					}
					return
				}
				t.Fatalf("must not be empty\n")
			})

			t.Run("many-1k", func(t *testing.T) {
				t.Parallel()

				var tiny []byte = []byte("helo")
				var b1k iter.Seq2[bg.Blob, error] = func(
					yield func(bg.Blob, error) bool,
				) {
					for range 1000 {
						if !yield(tiny, nil) {
							return
						}
					}
				}

				var gz bytes.Buffer
				e := bg.BlobsToWriter(&gz)(bg.Blobs(b1k))
				if nil != e {
					t.Fatalf("unexpected error: %v\n", e)
				}

				var blobs iter.Seq2[bg.Blob, error] = bg.GzipToBlobs(
					bufio.NewReader(bytes.NewReader(gz.Bytes())),
				)
				var cnt int
				for blob, e := range blobs {
					if nil != e {
						t.Fatalf("unexpected error: %v\n", e)
					}
					var s string = string(blob)
					if s != "helo" {
						t.Fatalf("unexpected value(%v): %v\n", cnt, blob)
					}
					cnt += 1
				}
				if 1000 != cnt {
					t.Fatalf("unexpected count: %v\n", cnt)
				}
			})
		})
	})
}
