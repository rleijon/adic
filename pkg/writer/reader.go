package writer

import (
	"github.com/rleijon/adic/pkg/types"
	"golang.org/x/exp/mmap"
)

type AdicReader struct {
	dir           string
	columnReaders map[string]ColumnReader
}

type ColumnReader interface {
	ReadAt(int64) (interface{}, error)
	ReadLatest() (interface{}, error)
	FilterAll(ColumnFilter) ([]int64, []interface{})
	FilterIndices([]int64, ColumnFilter) ([]int64, []interface{})
	ReadIndices([]int64) []interface{}
	Count() int
	Start()
}

func NewReader(dir string, fields []types.Field) *AdicReader {
	readers := make(map[string]ColumnReader, len(fields))
	for _, v := range fields {
		readers[v.Name] = getColumnReader(dir, v)
		readers[v.Name].Start()
	}
	return &AdicReader{
		dir:           dir,
		columnReaders: readers,
	}
}

func getColumnReader(dir string, field types.Field) ColumnReader {
	if field.Type == types.IntType {
		return &IntColumnReader{
			Dir:        dir,
			ColumnName: field.Name,
		}
	} else if field.Type == types.StringType {
		return &StringColumnReader{
			Dir:        dir,
			ColumnName: field.Name,
		}
	}
	return nil
}
func (w *AdicReader) Count() int {
	for _, v := range w.columnReaders {
		return v.Count()
	}
	return 0
}

func (w *AdicReader) Read(f *Filter) ([]*types.Object, error) {
	var indices []int64
	results := make(map[int64]*types.Object)
	for _, v := range f.Columns {
		var vals []interface{}
		if indices == nil {
			indices, vals = w.columnReaders[v.ColumnName()].FilterAll(v)
			for i, vv := range indices {
				results[vv] = &types.Object{
					Values: map[string]interface{}{v.ColumnName(): vals[i]},
				}
			}
		} else {
			indices, vals = w.columnReaders[v.ColumnName()].FilterIndices(indices, v)
			for i, vv := range indices {
				results[vv].Values[v.ColumnName()] = vals[i]
			}
		}
	}
	for name, v := range w.columnReaders {
		isBreak := false
		for _, col := range f.Columns {
			if name == col.ColumnName() {
				isBreak = true
			}
		}
		if !isBreak {
			vals := v.ReadIndices(indices)
			for i, vv := range indices {
				results[vv].Values[name] = vals[i]
			}
		}
	}
	indexMap := make(map[int64]bool, len(indices))
	for _, v := range indices {
		indexMap[v] = true
	}
	resultArray := make([]*types.Object, 0)
	for k, v := range results {
		if _, ok := indexMap[k]; ok {
			resultArray = append(resultArray, v)
		}
	}
	return resultArray, nil
}

func (w *AdicReader) ReadAt(idx int64) (*types.Object, error) {
	result := &types.Object{
		Values: make(map[string]interface{}, len(w.columnReaders)),
	}
	for i, v := range w.columnReaders {
		vv, e := v.ReadAt(idx)
		result.Values[i] = vv
		if e != nil {
			return result, e
		}
	}
	return result, nil
}

func (w *AdicReader) ReadLatest() (*types.Object, error) {
	result := &types.Object{
		Values: make(map[string]interface{}, len(w.columnReaders)),
	}
	for i, v := range w.columnReaders {
		vv, e := v.ReadLatest()
		result.Values[i] = vv
		if e != nil {
			return result, e
		}
	}
	return result, nil
}

type IntColumnReader struct {
	Dir        string
	ColumnName string
	Reader     *mmap.ReaderAt
}

func (w *IntColumnReader) Start() {
	w.Reader, _ = mmap.Open(w.Dir + "/" + w.ColumnName + ".data")
}

func (w *IntColumnReader) FilterAll(filter ColumnFilter) ([]int64, []interface{}) {
	l := int64(w.Reader.Len())
	filteredIndices := make([]int64, 0)
	results := make([]interface{}, 0)
	for i := int64(0); i < l/4; i++ {
		vv, _ := readInt(w.Reader, i*4)
		v := int32(vv)
		if filter.Test(v) {
			filteredIndices = append(filteredIndices, i)
			results = append(results, v)
		}
	}
	return filteredIndices, results
}
func (w *IntColumnReader) FilterIndices(indices []int64, filter ColumnFilter) ([]int64, []interface{}) {
	results := make([]int64, 0)
	filteredIndices := make([]interface{}, 0)
	for _, i := range indices {
		v, _ := readInt(w.Reader, i*4)
		if filter.Test(v) {
			results = append(results, i)
			filteredIndices = append(filteredIndices, v)
		}
	}
	return results, filteredIndices
}

func (w *IntColumnReader) ReadIndices(indices []int64) []interface{} {
	results := make([]interface{}, 0)
	for _, i := range indices {
		v, _ := w.ReadAt(i)
		results = append(results, v)
	}
	return results
}
func (w *IntColumnReader) ReadLatest() (interface{}, error) {
	i, e := readInt(w.Reader, int64(w.Reader.Len()-4))
	return int32(i), e
}
func (w *IntColumnReader) ReadAt(i int64) (interface{}, error) {
	v, e := readInt(w.Reader, i*4)
	return int32(v), e
}

func (w *IntColumnReader) Count() int {
	return w.Reader.Len() / 4
}

type StringColumnReader struct {
	Dir         string
	ColumnName  string
	IndexReader *mmap.ReaderAt
	Reader      *mmap.ReaderAt
}

func (w *StringColumnReader) ReadLatest() (interface{}, error) {
	lastOffset, e := readInt(w.IndexReader, int64(w.IndexReader.Len()-4))
	firstOffset := uint32(0)
	if w.Reader.Len() >= 8 {
		firstOffset, e = readInt(w.IndexReader, int64(w.IndexReader.Len()-8))
	}
	bts := make([]byte, lastOffset-firstOffset)
	w.Reader.ReadAt(bts, int64(firstOffset))
	return string(bts), e
}

func (w *StringColumnReader) Count() int {
	return w.IndexReader.Len() / 4
}

func (w *StringColumnReader) ReadAt(i int64) (interface{}, error) {
	lastOffset, e := readInt(w.IndexReader, int64(i*4))
	firstOffset := uint32(0)
	if i > 0 {
		firstOffset, e = readInt(w.IndexReader, int64((i-1)*4))
	}
	if firstOffset > lastOffset {
		panic("First offset greater than last offset")
	}
	bts := make([]byte, lastOffset-firstOffset)
	w.Reader.ReadAt(bts, int64(firstOffset))
	return string(bts), e
}

func (w *StringColumnReader) FilterAll(filter ColumnFilter) ([]int64, []interface{}) {
	l := int64(w.IndexReader.Len())
	indices := make([]int64, 0)
	results := make([]interface{}, 0)
	for i := int64(0); i < l/4; i++ {
		v, _ := w.ReadAt(i)
		if filter.Test(v) {
			indices = append(indices, i)
			results = append(results, v)
		}
	}
	return indices, results
}

func (w *StringColumnReader) FilterIndices(indices []int64, filter ColumnFilter) ([]int64, []interface{}) {
	filteredIndices := make([]int64, 0)
	results := make([]interface{}, 0)
	for _, i := range indices {
		v, _ := w.ReadAt(i)
		if filter.Test(v) {
			filteredIndices = append(filteredIndices, i)
			results = append(results, v)
		}
	}
	return filteredIndices, results
}

func (w *StringColumnReader) ReadIndices(indices []int64) []interface{} {
	results := make([]interface{}, 0)
	for _, i := range indices {
		v, _ := w.ReadAt(i)
		results = append(results, v)
	}
	return results
}

func (w *StringColumnReader) Start() {
	w.Reader, _ = mmap.Open(w.Dir + "/" + w.ColumnName + ".data")
	w.IndexReader, _ = mmap.Open(w.Dir + "/" + w.ColumnName + "-index.data")
}
