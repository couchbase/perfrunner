package main

import (
	"testing"
)

func TestRandString(t *testing.T) {
	var arr [5][]interface{}
	for i := 0; i < 5; i++ {
		arr[i] = RandString(12, "000125000000", "000175000000")
		for j := 0; j < i; j++ {
			if arr[i][0] == arr[j][0] {
				t.Fail()
			}
		}
	}

	for i := 0; i < 5; i++ {
		arr[i] = RandString(6, "00abcd", "0abcde")
		for j := 0; j < i; j++ {
			if arr[i][0] == arr[j][0] {
				t.Fail()
			}
		}
	}
}
