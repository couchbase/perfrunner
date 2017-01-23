package main

import (
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/couchbase/go-couchbase"
)

const (
	prime = int64(971)
)

var (
	numDocs, numWorkers, size int64
	prefix, doc_type          string
	couchbaseBucket           *couchbase.Bucket
)

type KV struct {
	key   string
	value *doc
}

// generateKV produces unique key-value pairs. The entire key space is
// equally divided between multiple workers. Documents are generated so that
// the overall order of insertions is deterministic and pseudo-random.
func generateKV(workerID int64) chan KV {
	kvPairs := make(chan KV, 1e4)
	var docsPerWorker = numDocs / numWorkers
	var j int64

	go func() {
		defer close(kvPairs)

		var doc_gen get_doc_value
		doc_gen = basic_doc{}

		// Will add more types of doc as they come along
		switch doc_type {
		case "multi_item_doc":
			doc_gen = multi_item_doc{}
		case "fixed_size_doc":
			doc_gen = fixed_size_doc{}
		}

		for i := int64(0); i < docsPerWorker; i++ {
			j = (j + prime) % docsPerWorker
			seed := j + workerID*docsPerWorker

			kvPairs <- KV{newKey(seed), doc_gen.newValue(seed)}
		}
	}()

	return kvPairs
}

// singleLoad represents an individual sequence if SET operations.
func singleLoad(workerID int64, wg *sync.WaitGroup) {
	defer wg.Done()

	for kv := range generateKV(workerID) {
		couchbaseBucket.Set(kv.key, 0, kv.value)
	}
}

// load spawns multiple workload generators and waits until all sub-routines are
// done.
func load() {
	wg := sync.WaitGroup{}

	for i := int64(0); i < numWorkers; i++ {
		wg.Add(1)
		go singleLoad(i, &wg)
	}

	wg.Wait()
}

// getBucket connects to a cluster of Couchbase Server instances and returns a
// pointer to the Bucket object.
func getBucket(hostname, bucket, password string) (*couchbase.Bucket, error) {
	var connStr = fmt.Sprintf("http://%s:8091/", hostname)

	c, err := couchbase.ConnectWithAuthCreds(connStr, bucket, password)
	if err != nil {
		return nil, err
	}

	pool, err := c.GetPool("default")
	if err != nil {
		return nil, err
	}

	return pool.GetBucketWithAuth(bucket, bucket, password)
}

func main() {
	var hostname, bucket, password string
	flag.StringVar(&hostname, "hostname", "127.0.0.1", "hostname")
	flag.StringVar(&bucket, "bucket", "bucket-1", "bucket name")
	flag.StringVar(&password, "password", "password", "bucket password")

	flag.StringVar(&prefix, "prefix", "couchbase", "common key prefix")
	flag.Int64Var(&numDocs, "docs", 1e8, "number of documents")
	flag.Int64Var(&numWorkers, "workers", 125, "number of workers")
	flag.Int64Var(&size, "size", 128, "size of doc")
	flag.StringVar(&doc_type, "doc_type", "basic", "type of document")

	flag.Parse()

	var err error
	couchbaseBucket, err = getBucket(hostname, bucket, password)
	if err != nil {
		fmt.Printf("Failed to connect to Couchbase Server: %v\n", err)
		os.Exit(1)
	}

	load()
}
