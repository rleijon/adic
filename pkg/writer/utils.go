package writer

import "golang.org/x/exp/mmap"

func readInt(reader *mmap.ReaderAt, start int64) (uint32, error) {
	var b [4]byte
	_, e := reader.ReadAt(b[:], int64(start))
	v := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return v, e
}
