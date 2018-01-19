package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	couchbase "github.com/couchbase/go-couchbase"
)

const (
	bufferSize = 1e3
)

type tableMeta struct {
	keyFormat  string
	rangeStart int64
}

type kv struct {
	key   string
	value interface{}
}

// parseTableMeta reads socialGen metadata and extracts the initial decimal
// identifier and the key format for the given table.
func parseTableMeta(metaPath string) (*tableMeta, error) {
	meta := tableMeta{}

	file, err := os.OpenFile(metaPath, os.O_RDONLY, 0644)
	if err != nil {
		return &meta, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	meta.rangeStart, err = extractRangeStart(scanner.Text())
	if err != nil {
		return &meta, err
	}

	scanner.Scan()
	meta.keyFormat, err = extractKeyFormat(scanner.Text())
	if err != nil {
		return &meta, err
	}
	return &meta, err
}

// extractRangeStart extracts the decimal identifier of the first key in the
// key space. socialGen provides a range of the keys (e.g.,
// "IDRange=3800001:3820071"). This function returns the begginnf of the range
// (e.g., 3800001)
func extractRangeStart(meta string) (int64, error) {
	//
	settings := strings.Split(meta, "=")
	if len(settings) != 2 {
		return 0, fmt.Errorf("invalid range: %s", meta)
	}
	ranges := strings.Split(settings[1], ":")
	if len(ranges) != 2 {
		return 0, fmt.Errorf("invalid range: %s", meta)
	}
	return strconv.ParseInt(ranges[0], 10, 64)
}

// extractKeyFormat extracts a format specifier that will used for the keys
// (e.g., IDFormat=%015d).
func extractKeyFormat(meta string) (string, error) {
	settings := strings.Split(meta, "=")
	if len(settings) != 2 {
		return "", fmt.Errorf("invalid key format: %s", meta)
	}
	return settings[1], nil
}

// copyBytes creates a copy of byte array
func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// readDocs sequentially scans the socialGen data files.
func readDocs(jsonPath string) <-chan []byte {
	docs := make(chan []byte, bufferSize)

	go func() {
		defer close(docs)

		file, err := os.OpenFile(jsonPath, os.O_RDONLY, 0644)
		if err != nil {
			log.Fatalln(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			docs <- copyBytes(scanner.Bytes())
		}

		if err := scanner.Err(); err != nil {
			log.Fatalln(err)
		}
	}()

	return docs
}

// unmarshal deserializes the socialGen documents.
func unmarshal(doc []byte) (interface{}, error) {
	var j interface{}
	err := json.Unmarshal(doc, &j)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling error: %v", err)
	}
	return j, nil
}

// generateJSON creates a pipeline of the deserialized socialGen documents.
func generateJSON(docs <-chan []byte) <-chan interface{} {
	jsons := make(chan interface{}, bufferSize)

	go func() {
		defer close(jsons)

		for doc := range docs {
			j, err := unmarshal(doc)
			if err != nil {
				log.Fatalln(err)
			}
			jsons <- j
		}
	}()

	return jsons
}

// generatePayload adds keys to the incoming pipeline of the JSON documents.
func generatePayload(meta *tableMeta, values <-chan interface{}) <-chan kv {
	payload := make(chan kv, 1e3)

	go func() {
		defer close(payload)

		keyId := meta.rangeStart
		for value := range values {
			key := fmt.Sprintf(meta.keyFormat, keyId)
			keyId++
			payload <- kv{key, value}
		}
	}()

	return payload
}

// getBucket establishes a new Couchbase Server connection.
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

// load inserts new documents into Couchbase Server database.
func load(bucket *couchbase.Bucket, payload <-chan kv, wg *sync.WaitGroup) {
	defer wg.Done()

	for payload := range payload {
		if err := bucket.Set(payload.key, 0, payload.value); err != nil {
			log.Fatalln(err)
		}
	}
}

// runWorkers starts and orchestrates multiple workers; each worker inserts new
// documents into Couchbase Server database.
func runWorkers(hostname, bucket, password string, workers int, payload <-chan kv) {
	couchbaseBucket, err := getBucket(hostname, bucket, password)
	if err != nil {
		log.Fatalf("failed to establish a database connection: %v\n", err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go load(couchbaseBucket, payload, &wg)
	}
	wg.Wait()
}

func main() {
	var hostname, bucket, password string
	flag.StringVar(&hostname, "hostname", "127.0.0.1", "hostname")
	flag.StringVar(&bucket, "bucket", "bucket-1", "bucket name")
	flag.StringVar(&password, "password", "password", "bucket password")

	var dataPath, table string
	flag.StringVar(&dataPath, "path", "socialGen/data", "data path")
	flag.StringVar(&table, "table", "gbook_users", "BigFUN table name")

	var workers int
	flag.IntVar(&workers, "workers", 1, "number of workers")

	flag.Parse()

	metaPath := path.Join(dataPath, table+".meta")
	meta, err := parseTableMeta(metaPath)
	if err != nil {
		log.Fatalf("failed to read socialGen metadata: %v\n", err)
	}

	jsonPath := path.Join(dataPath, table+".json")
	docs := readDocs(jsonPath)
	values := generateJSON(docs)
	payload := generatePayload(meta, values)

	runWorkers(hostname, bucket, password, workers, payload)
}
