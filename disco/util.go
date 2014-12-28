package disco

import (
	"io"
)

//Stollen from http://golang.org/src/pkg/io/multi.go

type multiReader struct {
	readers []io.ReadCloser
}

func (mr *multiReader) Read(p []byte) (n int, err error) {
	for len(mr.readers) > 0 {
		n, err = mr.readers[0].Read(p)
		if n > 0 || err != io.EOF {
			if err == io.EOF {
				// Don't return EOF yet. There may be more bytes
				// in the remaining readers.
				err = nil
				//Close current reader
				mr.readers[0].Close()
				//Since we closed this reader, we move to next
				mr.readers = mr.readers[1:]
			}
			return
		}
		mr.readers = mr.readers[1:]
	}
	return 0, io.EOF
}

//Implement Closer, close remaining readers in case user aborts early
func (mr *multiReader) Close() error {
	for _, rdr := range mr.readers {
		rdr.Close()
	}
	return nil
}

// MultiReader returns a Reader that's the logical concatenation of
// the provided input readers.  They're read sequentially.  Once all
// inputs have returned EOF, Read will return EOF.  If any of the readers
// return a non-nil, non-EOF error, Read will return that error.
func MultiReadCloser(readers []io.ReadCloser) io.ReadCloser {
	//r := make([]io.ReadCloser, len(readers))
	//copy(r, readers)
	return &multiReader{readers}
}
