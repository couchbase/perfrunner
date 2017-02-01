package main

import (
	"encoding/json"
	"testing"
)

func BenchmarkNewKey(b *testing.B) {
	prefix = "prefix-1"
	for n := 0; n < b.N; n++ {
		newKey(int64(n))
	}
}

func BenchmarkNewValueBasic(b *testing.B) {
	doc_gen := basic_doc{}
	for n := 0; n < b.N; n++ {
		doc_gen.newValue(int64(n))
	}
}

func BenchmarkNewValueFixedSize(b *testing.B) {
	size = 64
	doc_gen := fixed_size_doc{}
	for n := 0; n < b.N; n++ {
		doc_gen.newValue(int64(n))
	}
}

func BenchmarkNewValueMultiItem(b *testing.B) {
	size = 1024
	doc_gen := multi_item_doc{}
	for n := 0; n < b.N; n++ {
		doc_gen.newValue(int64(n))
	}
}

func fuzzyEqual(v1, v2 int, e int) bool {
	d := v1 - v2
	return (d >= 0 && d <= e) || (d < 0 && -d <= e)
}

func TestValueSize(t *testing.T) {
	doc_gen := multi_item_doc{}

	for _, s := range []int64{256, 512, 1024, 2048} {
		size = s
		v := doc_gen.newValue(size)
		b, err := json.Marshal(&v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !fuzzyEqual(len(b), int(s), 1) {
			t.Errorf("expected %v, got %v", size, len(b))
		}
	}
}
