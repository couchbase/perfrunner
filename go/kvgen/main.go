package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/couchbase/go-couchbase"
)

const (
	prime = uint64(971)
)

var (
	numDocs, numWorkers uint64
	prefix              string
	couchbaseBucket     *couchbase.Bucket
)

type doc struct {
	Email string `json:"email"`
}

type KV struct {
	key   string
	value *doc
}

// newKey generates a new key with a common prefix and variable suffix based on
// sequential document identifier.
func newKey(i uint64) string {
	return fmt.Sprintf("%s-%012x", prefix, i)
}

// newValue generates a new set of field(s) based on sequential document
// identifier. Current time is used in order to randomize document fields.
func newValue(i uint64) *doc {
	t := time.Now()

	return &doc{
		Email: fmt.Sprintf("%x@%09x.%d", t.UnixNano(), i, i%10),
	}
}

// generateKV produces unique key-value pairs. The entire key space is
// equally divided between multiple workers. Documents are generated so that
// the overall order of insertions is deterministic and pseudo-random.
func generateKV(workerID uint64) chan KV {
	kvPairs := make(chan KV, 1e4)
	var docsPerWorker = numDocs / numWorkers
	var j uint64

	go func() {
		defer close(kvPairs)

		for i := uint64(0); i < docsPerWorker; i++ {
			j = (j + prime) % docsPerWorker
			seed := j + workerID*docsPerWorker

			kvPairs <- KV{newKey(seed), newValue(seed)}
		}
	}()

	return kvPairs
}

// singleLoad represents an individual sequence if SET operations.
func singleLoad(workerID uint64, wg *sync.WaitGroup) {
	defer wg.Done()

	for kv := range generateKV(workerID) {
		couchbaseBucket.Set(kv.key, 0, kv.value)
	}
}

// load spawns multiple workload generators and waits until all sub-routines are
// done.
func load() {
	wg := sync.WaitGroup{}

	for i := uint64(0); i < numWorkers; i++ {
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
	flag.Uint64Var(&numDocs, "docs", 1e8, "number of documents")
	flag.Uint64Var(&numWorkers, "workers", 125, "number of workers")

	flag.Parse()

	var err error
	couchbaseBucket, err = getBucket(hostname, bucket, password)
	if err != nil {
		fmt.Printf("Failed to connect to Couchbase Server: %v\n", err)
		os.Exit(1)
	}

	load()
}
