package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/couchbase/gocb"
)

type Query struct {
	Name    string  "json:`name`"
	Query   string  "json:`query`"
	Indexes []Index "json:`indexes`"
}

type Index struct {
	Name         string   "json:`name`"
	Definition   []string "json:`definition`"
	DropOnFinish bool     "json:`dropOnFinish`"
}

type Configfile struct {
	Concurrency   []int   `json:"concurrency"`
	CreatePrimary bool    `json:"createPrimary"`
	Bucket        string  `json:"bucket"`
	Cbhost        string  `json:"cbhost"`
	Password      string  `json:"password"`
	User          string  `json:"user"`
	Indexes       []Index `json:"indexes"`
	Queries       []Query `json:"queries"`
}

var configuration = flag.String("i", "config.json", "Path to the configuration file")
var output = flag.String("o", "report.csv", "Output file")
var showHelp = flag.Bool("?", false, "Shows CLI help")
var queryTimeout = flag.Duration("t", (30 * time.Second), "Query timeout")
var maxProcs = flag.Int("c", 0, "Max concurrency")
var executionConfig Configfile
var bucket *gocb.Bucket
var phaseCount = 0
var errorCount = 0

func phase(message string) {
	phaseCount += 1
	log.Printf("Phase %d %s", phaseCount, message)
}

func createIndex(name string, definition []string) {
	log.Printf("Creating index %s", name)
	err := bucket.Manager(executionConfig.User, executionConfig.Password).CreateIndex(name, definition, true, false)
	if err != nil {
		log.Print(err)
	}
}

func dropIndex(name string) {
	log.Printf("Removing index %s", name)
	err := bucket.Manager(executionConfig.User, executionConfig.Password).DropIndex(name, true)
	if err != nil {
		log.Print(err)
	}
}

func createPrimaryIndex() {
	log.Print("Creating primary index")
	err := bucket.Manager(executionConfig.User, executionConfig.Password).CreatePrimaryIndex("primary", false, false)
	if err != nil {
		log.Print(err)
	}
}

func executeQuery(query string, wg *sync.WaitGroup) {
	n1qlQuery := gocb.NewN1qlQuery(query)
	n1qlQuery.Timeout(*queryTimeout)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, []interface{}{})
	if err != nil {
		log.Print(err)
	} else {
		results.Close()
		errorCount += 1
	}
	wg.Done()
}

func testQuery(query string, name string, concurrentCalls int) {
	var wg sync.WaitGroup
	startingTime := time.Now()
	for i := 0; i < concurrentCalls; i++ {
		wg.Add(1)
		go executeQuery(query, &wg)
	}
	wg.Wait()
	took := time.Since(startingTime)
	log.Printf("----Query %s[%d]: Took %s", name, concurrentCalls, took)
}

func startExecutionPlan() {
	connectionStr := "couchbase://" + executionConfig.Cbhost
	log.Printf("Connecting to bucket %s in cluster %s and query timeout is %s", executionConfig.Bucket, connectionStr, *queryTimeout)
	cluster, err := gocb.Connect(connectionStr)
	if err != nil {
		log.Print(err)
		log.Panic("Cannot connect to cluster")
	}
	cluster.SetConnectTimeout(2 * time.Second)
	cbBucket, berr := cluster.OpenBucket(executionConfig.Bucket, executionConfig.Password)
	if berr != nil {
		log.Print(berr)
		log.Panic("Cannot connect to bucket")
	}
	bucket = cbBucket
	if executionConfig.CreatePrimary == true {
		phase("Creating primary index")
		createPrimaryIndex()
	}
	phase("Creating common indexes")
	for _, v := range executionConfig.Indexes {
		createIndex(v.Name, v.Definition)
	}
	phase("Benchmarking queries")
	for _, stmt := range executionConfig.Queries {
		for _, ndx := range stmt.Indexes {
			createIndex(ndx.Name, ndx.Definition)
		}
		for _, v := range executionConfig.Concurrency {
			log.Printf("Testing q=%s with concurrency %d", stmt.Name, v)
			testQuery(stmt.Query, stmt.Name, v)
		}
		for _, ndx := range stmt.Indexes {
			if ndx.DropOnFinish == true {
				dropIndex(ndx.Name)
			}
		}
	}
	phase("Removing common indexes")
	for _, v := range executionConfig.Indexes {
		dropIndex(v.Name)
	}
	log.Printf("Total errors found: %d", errorCount)
}

func parseConfig() {
	data, err := ioutil.ReadFile(*configuration)
	if err != nil {
		log.Print(err)
		log.Panicf("Cannot open the configuration file %s", *configuration)
	}
	err = json.Unmarshal(data, &executionConfig)
	if err != nil {
		log.Print(err)
		log.Panic("Invalid configuration file or format")
	}
}

func main() {
	flag.Parse()
	if *showHelp == true {
		flag.PrintDefaults()
	} else {
		log.Printf("Concurrency %d", *maxProcs)
		runtime.GOMAXPROCS(*maxProcs)
		parseConfig()
		startExecutionPlan()
	}
}
