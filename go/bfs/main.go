package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"unicode/utf8"

	"gopkg.in/couchbase/gocb.v1"
)

const (
	bufferSize = 1e3
)

var (
	couchbaseBucket *gocb.Bucket
)

// getBucket connects to a cluster of Couchbase Server instances and returns a
// pointer to the Bucket object.
func getBucket(hostname, bucket, password string) (*gocb.Bucket, error) {
	var connStr = fmt.Sprintf("couchbase://%s", hostname)
	cluster, err := gocb.Connect(connStr)
	if err != nil {
		return nil, err
	}
	return cluster.OpenBucket(bucket, password)
}

func reverse(s string) string {
	size := len(s)
	buf := make([]byte, size)
	for start := 0; start < size; {
		r, n := utf8.DecodeRuneInString(s[start:])
		start += n
		utf8.EncodeRune(buf[size-start:], r)
	}
	return string(buf)
}

// reverseAttribute reveres document fields identified by primary key
func reverseAttribute(key, path string) error {
	lookupInBuilder := couchbaseBucket.LookupIn(key)
	lookupInBuilder = lookupInBuilder.Get(path)
	documentFragment, err := lookupInBuilder.Execute()
	if err != nil {
		return err
	}

	var value string
	if err = documentFragment.ContentByIndex(0, &value); err != nil {
		return err
	}

	mb := couchbaseBucket.MutateIn(key, documentFragment.Cas(), 0)
	mb = mb.Upsert(path, reverse(value), false)
	documentFragment, err = mb.Execute()
	return err
}

// generateKeys generates a sequence of document keys
func generateKeys(start, end int64) chan string {
	ids := make(chan string, bufferSize)
	go func() {
		for i := start; i <= end; i++ {
			ids <- fmt.Sprintf("%015d", i)
		}
	}()
	return ids
}

func gbookUsers() chan string {
	return generateKeys(1, 20000000)
}

func gbookMessages() chan string {
	return generateKeys(400000001, 499997119)
}

func chirpMessages() chan string {
	return generateKeys(1200000001, 1400002740)
}

type message struct {
	key, path string
}

func mutateGbookUsers() chan message {
	messages := make(chan message, bufferSize)
	go func() {
		for key := range gbookUsers() {
			messages <- message{key, "user_since"}
		}
	}()
	return messages
}

func mutateGbookMessages() chan message {
	messages := make(chan message, bufferSize)
	go func() {
		for key := range gbookMessages() {
			messages <- message{key, "author_id"}
		}
	}()
	return messages
}

func mutateChirpMessages() chan message {
	messages := make(chan message, bufferSize)
	go func() {
		for key := range chirpMessages() {
			messages <- message{key, "send_time"}
		}
	}()
	return messages
}

func mutate(messages chan message, wg *sync.WaitGroup) {
	for m := range messages {
		reverseAttribute(m.key, m.path)
	}
	wg.Done()
}

func mutateGroup(messages chan message, workers int) {
	wg := sync.WaitGroup{}
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go mutate(messages, &wg)
	}

	wg.Wait()
}

func mutateAll(workers int) {
	mutateGroup(mutateGbookUsers(), workers)
	mutateGroup(mutateGbookMessages(), workers)
	mutateGroup(mutateChirpMessages(), workers)
}

func main() {
	var hostname, bucket, password string
	var workers int

	flag.StringVar(&hostname, "hostname", "127.0.0.1", "hostname")
	flag.StringVar(&bucket, "bucket", "bucket-1", "bucket name")
	flag.StringVar(&password, "password", "password", "bucket password")
	flag.IntVar(&workers, "workers", 48, "number of workers")

	flag.Parse()

	var err error
	couchbaseBucket, err = getBucket(hostname, bucket, password)
	if err != nil {
		fmt.Printf("Failed to connect to Couchbase Server: %v\n", err)
		os.Exit(1)
	}

	mutateAll(workers)
}
