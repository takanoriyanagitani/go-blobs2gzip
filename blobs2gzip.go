package blobs2gzip

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"iter"
)

// Opaque data.
type Blob []byte

type Blobs iter.Seq2[Blob, error]

// Writes all [Blobs] to the writer.
func BlobsToWriter(wtr io.Writer) func(Blobs) error {
	return func(blobs Blobs) error {
		var gw *gzip.Writer = gzip.NewWriter(wtr)

		for blob, e := range blobs {
			if nil != e {
				return e
			}
			_, e := gw.Write(blob)
			if nil != e {
				return errors.Join(e, gw.Close())
			}

			e = gw.Close()
			if nil != e {
				return e
			}

			gw.Reset(wtr)
		}

		return nil
	}
}

const BlobSizeMaxDefault int64 = 1048576

// Reads concatenated gzips as blobs.
func GzipToBlobsLimited(limit int64) func(*bufio.Reader) iter.Seq2[Blob, error] {
	return func(rdr *bufio.Reader) iter.Seq2[Blob, error] {
		return func(yield func(Blob, error) bool) {
			zr, e := gzip.NewReader(rdr)
			if io.EOF == e {
				return
			}
			if nil != e {
				yield(nil, e)
				return
			}
			defer zr.Close()

			var buf bytes.Buffer
			for {
				zr.Multistream(false)
				buf.Reset()

				limited := &io.LimitedReader{
					R: zr,
					N: limit,
				}
				_, e := io.Copy(&buf, limited)
				if nil != e {
					yield(nil, e)
					return
				}

				if !yield(buf.Bytes(), nil) {
					return
				}

				e = zr.Reset(rdr)
				if io.EOF == e {
					return
				}

				if nil != e {
					yield(nil, e)
					return
				}
			}
		}
	}
}

// Reads concatenated gzips as blobs.
func GzipToBlobs(rdr *bufio.Reader) iter.Seq2[Blob, error] {
	return GzipToBlobsLimited(BlobSizeMaxDefault)(rdr)
}
