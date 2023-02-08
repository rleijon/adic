package writer

import (
	"bufio"
	"os"

	"github.com/rleijon/adic/pkg/types"
)

type Close func()

type AdicWriter struct {
	dir           string
	columnWriters map[string]ColumnWriter
}

type ColumnWriter interface {
	Write(interface{}) error
	Start()
	Flush()
}

func New(dir string, fields []types.Field) *AdicWriter {
	writers := make(map[string]ColumnWriter, len(fields))
	for _, v := range fields {
		writers[v.Name] = getColumnWriter(dir, v)
		writers[v.Name].Start()
	}
	return &AdicWriter{
		dir:           dir,
		columnWriters: writers,
	}
}

func getColumnWriter(dir string, field types.Field) ColumnWriter {
	if field.Type == types.IntType {
		return &IntColumnWriter{
			Dir:        dir,
			ColumnName: field.Name,
		}
	} else if field.Type == types.StringType {
		return &StringColumnWriter{
			Dir:        dir,
			ColumnName: field.Name,
		}
	}
	return nil
}

func (w *AdicWriter) Flush() {
	for _, v := range w.columnWriters {
		v.Flush()
	}
}
func (w *AdicWriter) Write(object types.Object) error {
	for i, v := range object.Values {
		e := w.columnWriters[i].Write(v)
		if e != nil {
			return e
		}
	}
	return nil
}

type IntColumnWriter struct {
	Dir        string
	ColumnName string
	Writer     bufio.Writer
	OnClose    Close
}

func (w *IntColumnWriter) Write(value interface{}) error {
	var b [4]byte
	v := value.(int32)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	_, e := w.Writer.Write(b[:])
	return e
}

func (w *IntColumnWriter) Flush() {
	w.Writer.Flush()
}

func (w *IntColumnWriter) Start() {
	fl, _ := os.Create(w.Dir + "/" + w.ColumnName + ".data")
	w.Writer = *bufio.NewWriter(fl)
	w.OnClose = func() {
		fl.Close()
	}
}

type StringColumnWriter struct {
	Dir         string
	ColumnName  string
	Offset      uint32
	Writer      bufio.Writer
	IndexWriter bufio.Writer
	OnClose     Close
}

func (w *StringColumnWriter) Write(value interface{}) error {
	v := len(value.(string))
	w.Offset += uint32(v)
	var b [4]byte
	b[0] = byte(w.Offset)
	b[1] = byte(w.Offset >> 8)
	b[2] = byte(w.Offset >> 16)
	b[3] = byte(w.Offset >> 24)
	_, e := w.IndexWriter.Write(b[:])
	w.Writer.WriteString(value.(string))
	return e
}

func (w *StringColumnWriter) Start() {
	fl, _ := os.Create(w.Dir + "/" + w.ColumnName + ".data")
	w.Writer = *bufio.NewWriter(fl)
	fl2, _ := os.Create(w.Dir + "/" + w.ColumnName + "-index.data")
	w.IndexWriter = *bufio.NewWriter(fl2)
	w.OnClose = func() {
		fl.Close()
		fl2.Close()
	}
}

func (w *StringColumnWriter) Flush() {
	w.Writer.Flush()
	w.IndexWriter.Flush()
}
