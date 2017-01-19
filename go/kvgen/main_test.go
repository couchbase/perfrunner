package main

import (
	"testing"
)

func BenchmarkNewKey(b *testing.B) {
	prefix = "prefix-1"
	for n := 0; n < b.N; n++ {
		newKey(uint64(n))
	}
}

func BenchmarkNewValue(b *testing.B) {
	for n := 0; n < b.N; n++ {
		newValue(uint64(n))
	}
}
