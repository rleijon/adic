package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/rleijon/adic/pkg/types"
	"github.com/rleijon/adic/pkg/writer"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func main() {
	fmt.Println("all data is columnary")
	rand.Seed(int64(time.Now().Nanosecond()))
	fields := []types.Field{
		{Name: "a", Type: types.IntType},
		{Name: "foo", Type: types.IntType},
		{Name: "b", Type: types.StringType},
		{Name: "c", Type: types.StringType},
	}
	r := writer.NewReader("Foo", fields)
	fltr := writer.Filter{Columns: []writer.ColumnFilter{
		&writer.EqualFilter{Name: "a", Value: int32(14)},
		&writer.EqualFilter{Name: "c", Value: "a"},
	},
	}
	start := time.Now()
	vals, _ := r.Read(&fltr)
	fmt.Println("Read", len(vals), "in", time.Since(start).Nanoseconds(), "ns")
}
