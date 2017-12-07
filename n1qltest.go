package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/gocb"
)

type Stats struct{
	Accumulated time.Duration "json:`accumulated`"
	SuccessCount int "json:`successCount`"
}

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
	Repetitions int `json:"repetitions"`
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
var explainOutput = flag.String("e", "explain.txt", "Explain file")
var showHelp = flag.Bool("?", false, "Shows CLI help")
var queryTimeout = flag.Duration("t", (30 * time.Second), "Query timeout")
var maxProcs = flag.Int("c", 1, "Max concurrency")
var executionConfig Configfile
var bucket *gocb.Bucket
var phaseCount = 0
var errorCount = 0
var file *os.File
var explainFile *os.File
var writer *csv.Writer
var repetitions = 1

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

func executeQuery(query string, loopCount int, stats Stats, wg *sync.WaitGroup) {
	n1qlQuery := gocb.NewN1qlQuery(query)
	n1qlQuery.Timeout(*queryTimeout)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, []interface{}{})
	if err != nil {
		errorCount += 1
		log.Print(err)
	} else {
		results.Close()
		stats.Accumulated+=results.Metrics().ExecutionTime

	}
	loopCount += 1
	if (loopCount<repetitions){
		executeQuery(query, loopCount, stats, nil)
	}
	if wg!= nil{
		wg.Done()
	}
}

func addReport(queryName string, concurrentCalls int, duration time.Duration) {
	var line = []string{queryName, strconv.Itoa(concurrentCalls), duration.String()}
	e := writer.Write(line)
	writer.Flush()
	if e != nil {
		log.Print(e)
	}
}

func addExplainReport(query string, report string) {
	explainFile.WriteString("\n" + query + "\n")
	explainFile.WriteString(report)
}

func explainIt(query string) {
	q := "explain " + query
	log.Printf("Explaining %s", query)
	n1qlQuery := gocb.NewN1qlQuery(q)
	n1qlQuery.Timeout(*queryTimeout)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, []interface{}{})
	if err != nil {
		errorCount += 1
		log.Print(err)
	} else {
		results.Close()
		var r interface{}
		results.Next(&r)
		b, _ := json.Marshal(r)
		addExplainReport(query, string(b))
	}
}

func testQuery(query string, name string, concurrentCalls int) {
	var wg sync.WaitGroup
	startingTime := time.Now()
	for i := 0; i < concurrentCalls; i++ {
		wg.Add(1)
		var groupStats Stats
		go executeQuery(query, 0, stats, &wg)
	}
	wg.Wait()
	took := time.Since(startingTime)
	addReport(name, concurrentCalls, took)
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
	phase("Creating output file")
	var fileError error
	file, fileError = os.Create(*output)
	if fileError != nil {
		log.Print("Error creating output file")
		log.Panic(fileError)
	}
	writer = csv.NewWriter(file)
	explainFile, fileError = os.Create(*explainOutput)
	phase("Benchmarking queries")
	for _, stmt := range executionConfig.Queries {
		println("\n")
		for _, ndx := range stmt.Indexes {
			createIndex(ndx.Name, ndx.Definition)
		}
		explainIt(stmt.Query)
		for _, v := range executionConfig.Concurrency {
			// log.Printf("Testing q=%s with concurrency %d", stmt.Name, v)
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
	defer writer.Flush()
	defer explainFile.Close()
	defer file.Close()
	log.Printf("Total errors found: %d", errorCount)
	log.Printf("Report %s is available", *output)
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
	repetitions = executionConfig.Repetitions
}

func main() {
	flag.Parse()
	if *showHelp == true {
		flag.PrintDefaults()
	} else {
		log.Printf("Concurrency %d out of %d CPUs will be used", *maxProcs, runtime.NumCPU())
		runtime.GOMAXPROCS(*maxProcs)
		parseConfig()
		startExecutionPlan()
	}
}
