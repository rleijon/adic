package writer

import (
	"math/rand"
	"testing"

	"github.com/rleijon/adic/pkg/types"
)

func BenchmarkWrite(b *testing.B) {
	fields := []types.Field{
		{Name: "a", Type: types.IntType},
		{Name: "foo", Type: types.IntType},
		{Name: "b", Type: types.StringType},
		{Name: "c", Type: types.StringType},
	}
	f := New("Foo", fields)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		obj := types.Object{
			Values: map[string]interface{}{
				"a":   int32(i),
				"foo": int32(i % 4),
				"b":   "hello",
				"c":   "testa",
			}}
		b.StartTimer()
		f.Write(obj)
	}
	f.Flush()
}

func BenchmarkRead(b *testing.B) {
	fields := []types.Field{
		{Name: "a", Type: types.IntType},
		{Name: "foo", Type: types.IntType},
		{Name: "b", Type: types.StringType},
		{Name: "c", Type: types.StringType},
	}
	f := NewReader("Foo", fields)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ReadAt(int64(rand.Int31n(int32(i) + 1)))
	}
}

func BenchmarkReadFilter(b *testing.B) {
	fields := []types.Field{
		{Name: "a", Type: types.IntType},
		{Name: "foo", Type: types.IntType},
		{Name: "b", Type: types.StringType},
		{Name: "c", Type: types.StringType},
	}
	f := NewReader("Foo", fields)
	fltr := &Filter{Columns: []ColumnFilter{
		&EqualFilter{Name: "b", Value: int32(3)},
		&EqualFilter{Name: "a", Value: int32(14)},
	},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Read(fltr)
	}
}
