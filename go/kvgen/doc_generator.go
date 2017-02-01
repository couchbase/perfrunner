package main

import (
	"fmt"
	"time"
)

func randSeq(n int64, step int64) string {
	ret := ""
	fmt_str := fmt.Sprintf("%%0%dx", step)
	t := time.Now()
	for i := int64(0); i < n; i += step {
		ret += fmt.Sprintf(fmt_str, t)
	}

	return ret[:n]
}

type doc struct {
	Email        string `json:"email,omitempty"`
	Name         string `json:"name,omitempty"`
	City         string `json:"city,omitempty"`
	Coins        int64  `json:"coins,omitempty"`
	Achievements int64  `json:"achievements,omitempty"`
	Body         string `json:"body,omitempty"`
}

// Here's a basic interface to get Value.
type get_doc_value interface {
	newValue(i int64) *doc
}

type basic_doc struct {
}

type fixed_size_doc struct {
}

type multi_item_doc struct {
}

// newKey generates a new key with a common prefix and variable suffix based on
// sequential document identifier.
func newKey(i int64) string {
	return fmt.Sprintf("%s-%012x", prefix, i)
}

// newValue generates a new set of field(s) based on sequential document
// identifier. Current time is used in order to randomize document fields.
func (basic basic_doc) newValue(i int64) *doc {
	t := time.Now().UnixNano()

	return &doc{
		Email: fmt.Sprintf("%x@%09x.%d", t, i, i%10),
	}
}

func (basic fixed_size_doc) newValue(i int64) *doc {
	return &doc{
		City: fmt.Sprintf("%s", randSeq(size, size)),
	}
}

func (basic multi_item_doc) newValue(i int64) *doc {
	t := time.Now().UnixNano()

	return &doc{
		Email:        fmt.Sprintf("%012x@%09x.%d", t, i, i%10),
		Name:         fmt.Sprintf("%07x %08x", t, t),
		City:         fmt.Sprintf("%09x", t),
		Coins:        t,
		Achievements: t % prime,
		Body:         fmt.Sprintf("%s", randSeq(size-166, 300)),
	}
}
