package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/go-faker/faker/v4"
)

var stdout = log.New(os.Stdout, ", ", log.Ldate|log.Lmicroseconds|log.Lmsgprefix)

func operationLogger(msg string) {
	stdout.Println(msg)
}

var params struct {
	host     string
	bucket   string
	username string
	password string

	keys               []string
	timeout            int
	configPollInterval int
	threads            int
	items              int
	runningTime        int
}

type Task struct {
	coll  *gocb.Collection
	index int
	mu    sync.Mutex
	stop  bool
}

func (t *Task) run(wg *sync.WaitGroup) {
	for !t.stop {
		key := t.nextKey()

		_, err := t.coll.Get(key, nil)
		if err != nil {
			operationLogger("[FAILURE] Read: " + err.Error())
			continue
		}

		_, err = t.coll.Upsert(key, getRandomDoc(), nil)

		if err != nil {
			operationLogger("[FAILURE] Store: " + err.Error())
		} else {
			operationLogger("[SUCCESS] Store: " + key)
		}
	}
	wg.Done()
}

func (t *Task) nextKey() string {
	t.mu.Lock()
	var key = params.keys[t.index]
	t.index = (t.index + 1) % params.items
	t.mu.Unlock()
	return key
}

func (t *Task) waiter(duration int) {
	time.Sleep(time.Duration(duration) * time.Second)
	t.stop = true
}

type SimpleData struct {
	Field1 string
	Field2 string
	Field3 string
	Field4 string
	Field5 string
}

func getRandomDoc() SimpleData {
	doc := SimpleData{}
	err := faker.FakeData(&doc)
	if err != nil {
		fmt.Println(err)
	}
	return doc
}

func argParse() {

	flag.StringVar(&params.host, "host", "localhost", "Cluster host address")
	flag.StringVar(&params.bucket, "bucket", "", "Bucket to use")
	flag.StringVar(&params.username, "username", "Administrator", "Username")
	flag.StringVar(&params.password, "password", "password", "Password")

	flag.IntVar(&params.timeout, "timeout", 2500, "Operation timeout")
	flag.IntVar(&params.configPollInterval, "c-poll-interval", 2500, "ConfigPollInterval")
	flag.IntVar(&params.threads, "num-threads", 1, "Number of threads")
	flag.IntVar(&params.items, "num-items", 0, "Number of items to interact with")
	flag.IntVar(&params.runningTime, "time", 5, "How long to run the program")

	var keys string
	flag.StringVar(&keys, "keys", "", "Doc keys to interact with")
	flag.Parse()

	params.keys = strings.Split(keys, ",")
	if params.items == 0 {
		params.items = len(params.keys)
	}
	fmt.Fprintf(os.Stderr, "Running benchmark for %d sec on host %s (%s) with [items: %d, threads: %d, timeout: %dus] \n",
		params.runningTime, params.host, params.bucket, params.items, params.threads, params.timeout)
}

func main() {
	argParse()

	gocb.SetLogger(gocb.VerboseStdioLogger())
	cluster, err := gocb.Connect("couchbase://"+params.host, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: params.username,
			Password: params.password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			KVTimeout: time.Duration(params.timeout) * time.Millisecond,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	bucket := cluster.Bucket(params.bucket)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}
	task := Task{
		coll:  bucket.Scope("_default").Collection("_default"),
		stop:  false,
		index: 0,
	}

	var waitGroup sync.WaitGroup
	for i := 0; i < params.threads; i++ {
		go task.run(&waitGroup)
	}
	waitGroup.Add(params.threads)
	go task.waiter(params.runningTime)
	waitGroup.Wait()
}
