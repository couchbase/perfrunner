// Tool receives raw events from dcp-client.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
)

import (
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
)

var options struct {
	buckets        []string // buckets to connect with
	kvAddress      []string
	stats          int // periodic timeout(ms) to print stats, 0 will disable
	printFLogs     bool
	auth           string
	info           bool
	debug          bool
	trace          bool
	numMessages    int
	outputFile     string
	numConnections int
}

var rch = make(chan []interface{}, 10000)

func argParse() string {
	var buckets string
	var kvAddress string

	flag.StringVar(&buckets, "buckets", "default",
		"buckets to listen")
	flag.StringVar(&kvAddress, "kvaddrs", "",
		"list of kv-nodes to connect")
	flag.IntVar(&options.stats, "stats", 1000,
		"periodic timeout in mS, to print statistics, `0` will disable stats")
	flag.BoolVar(&options.printFLogs, "flogs", false,
		"display failover logs")
	flag.StringVar(&options.auth, "auth", "",
		"Auth user and password")
	flag.BoolVar(&options.info, "info", false,
		"display informational logs")
	flag.BoolVar(&options.debug, "debug", false,
		"display debug logs")
	flag.BoolVar(&options.trace, "trace", false,
		"display trace logs")
	flag.IntVar(&options.numMessages, "nummessages", 1000000,
		"number of DCP messages to wait for")
	flag.StringVar(&options.outputFile, "outputfile", "/root/dcpstatsfile",
		"file to save dcp stats output")
	flag.IntVar(&options.numConnections, "numconnections", 4,
		"number of DCP messages to wait for")

	flag.Parse()

	options.buckets = strings.Split(buckets, ",")
	if options.debug {
		logging.SetLogLevel(logging.Debug)
	} else if options.trace {
		logging.SetLogLevel(logging.Trace)
	} else {
		logging.SetLogLevel(logging.Info)
	}
	if kvAddress == "" {
		logging.Fatalf("Please provide -kvaddrs")
	}
	options.kvAddress = strings.Split(kvAddress, ",")

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	return args[0]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <cluster-addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	cluster := argParse()

	// setup cbauth
	if options.auth != "" {
		up := strings.Split(options.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(cluster, up[0], up[1]); err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	for _, bucket := range options.buckets {
		go startBucket(cluster, bucket, &wg)
	}
	go receive()
	wg.Wait()
	logging.Infof("Test Completed\n")
}

func startBucket(cluster, bucketn string, wg *sync.WaitGroup) int {
	logging.Infof("Connecting with %q\n", bucketn)
	b, err := common.ConnectBucket(cluster, "default", bucketn)
	mf(err, "bucket")

	dcpConfig := map[string]interface{}{
		"genChanSize":    10000,
		"dataChanSize":   10000,
		"numConnections": options.numConnections,
		"activeVbOnly":   true,
	}
	flags := uint32(0x0)
	dcpFeed, err := b.StartDcpFeedOver(
		couchbase.NewDcpFeedName("rawupr"),
		uint32(0), flags, options.kvAddress, 0xABCD, dcpConfig)
	mf(err, "- upr")

	vbnos := listOfVbnos()
	uuid := common.GetUUID(fmt.Sprintf("BucketFailoverLog-%v", bucketn), 0xABCD)
	flogs, err := b.GetFailoverLogs(0xABCD, vbnos, uuid, dcpConfig)
	mf(err, "- dcp failoverlogs")

	if options.printFLogs {
		printFlogs(vbnos, flogs)
	}

	logging.Infof("options.messages = %d\n", options.numMessages)

	t0 := time.Now()
	go startDcp(dcpFeed, flogs, wg)

	cnt := 0
	for {
		e, ok := <-dcpFeed.C
		if ok == false {
			logging.Infof("Closing for bucket %q %d\n", b.Name, e.Cas)
			break
		}
		cnt++
		if cnt >= options.numMessages {
			break
		}
		rch <- []interface{}{b.Name, e}
	}
	t1 := time.Now()

	timeTaken := t1.Sub(t0)
	throughput := float64(options.numMessages) / timeTaken.Seconds()
	if cnt < options.numMessages {
		throughput = float64(0)
	}
	writeStatsFile(timeTaken, throughput)
	logging.Infof("Done startBucket\n")

	b.Close()

	defer wg.Done()
	return 0
}

func startDcp(dcpFeed *couchbase.DcpFeed, flogs couchbase.FailoverLog, wg *sync.WaitGroup) {
	start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	snapStart, snapEnd := uint64(0), uint64(0)
	manifest, scope, collection := "", "", []string{}
	for vbno, flog := range flogs {
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		opaque, flags, vbuuid := uint16(vbno), uint32(0), x[0]
		err := dcpFeed.DcpRequestStream(
			vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd, manifest, scope, collection)
		mf(err, fmt.Sprintf("stream-req for %v failed", vbno))
	}
	logging.Infof("Done startDCP\n")
	defer wg.Done()
}

func mf(err error, msg string) {
	if err != nil {
		logging.Fatalf("%v: %v", msg, err)
	}
}

func receive() {
	logging.Infof("receive() Beginning")
	// bucket -> Opcode -> #count
	counts := make(map[string]map[mcd.CommandCode]int)

	var tick <-chan time.Time
	if options.stats > 0 {
		tick = time.Tick(time.Millisecond * time.Duration(options.stats))
	}

loop:
	for {
		select {
		case msg, ok := <-rch:
			if ok == false {
				break loop
			}
			bucket, e := msg[0].(string), msg[1].(*mc.DcpEvent)
			if _, ok := counts[bucket]; !ok {
				counts[bucket] = make(map[mcd.CommandCode]int)
			}
			if _, ok := counts[bucket][e.Opcode]; !ok {
				counts[bucket][e.Opcode] = 0
			}
			counts[bucket][e.Opcode]++

		case <-tick:
			for bucket, m := range counts {
				logging.Infof("%q %s\n", bucket, sprintCounts(m))
			}
		}
	}
}

func sprintCounts(counts map[mcd.CommandCode]int) string {
	line := ""
	for i := 0; i < 256; i++ {
		opcode := mcd.CommandCode(i)
		if n, ok := counts[opcode]; ok {
			line += fmt.Sprintf("%s:%v ", mcd.CommandNames[opcode], n)
		}
	}
	return strings.TrimRight(line, " ")
}

func listOfVbnos() []uint16 {
	// list of vbuckets
	vbnos := make([]uint16, 0, 1024)
	for i := 0; i < 1024; i++ {
		vbnos = append(vbnos, uint16(i))
	}
	return vbnos
}

func printFlogs(vbnos []uint16, flogs couchbase.FailoverLog) {
	for i, vbno := range vbnos {
		logging.Infof("Failover log for vbucket %v\n", vbno)
		logging.Infof("   %#v\n", flogs[uint16(i)])
	}
	logging.Infof("\n")
}

func writeStatsFile(timeTaken time.Duration, throughput float64) {
	str := fmt.Sprintf("The call took %v seconds to run.\nThroughput = %f\n", timeTaken.Seconds(), throughput)
	// write the whole body at once
	err := ioutil.WriteFile(options.outputFile, []byte(str), 0777)
	if err != nil {
		panic(err)
	}
}
