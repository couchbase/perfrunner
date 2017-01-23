package main

import "testing"

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
