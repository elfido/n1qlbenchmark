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
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb"
)

type Stats struct {
	Accumulated  time.Duration
	SuccessCount int32
	ErrorCount   int32
	Average      float64
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
	Repetitions   int     `json:"repetitions"`
	CreatePrimary bool    `json:"createPrimary"`
	Bucket        string  `json:"bucket"`
	Cbhost        string  `json:"cbhost"`
	Password      string  `json:"password"`
	User          string  `json:"user"`
	Indexes       []Index `json:"indexes"`
	Queries       []Query `json:"queries"`
}

var configuration = flag.String("i", "config.json", "Path to the configuration file")
var executionOutput = flag.String("o", "report-execution.csv", "Output file")
var explainOutput = flag.String("p", "explain.txt", "Query explain plan file")
var showHelp = flag.Bool("?", false, "Shows CLI help :)")
var queryTimeout = flag.Duration("t", (30 * time.Second), "Query timeout")
var pause = flag.Duration("pause", 2*time.Second, "Pause between queries, this will help the cluster to recover")
var maxProcs = flag.Int("c", 1, "Max concurrency")
var executionConfig Configfile
var bucket *gocb.Bucket
var phaseCount = 0
var errorCount = 0
var repetitions = 1
var qSuccessCount int32
var qErrorCount int32
var qDuration int64

// Output files
var explainFile *os.File
var executionFile *os.File
var executionFileWriter *csv.Writer

func phase(message string) {
	phaseCount++
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

func executeQuery(query string, loopCount int, wg *sync.WaitGroup) {
	n1qlQuery := gocb.NewN1qlQuery(query)
	n1qlQuery.Timeout(*queryTimeout)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, []interface{}{})
	if err != nil {
		atomic.AddInt32(&qErrorCount, 1)
		errorCount++
	} else {
		results.Close()
		atomic.AddInt64(&qDuration, int64(results.Metrics().ExecutionTime))
		atomic.AddInt32(&qSuccessCount, 1)
	}
	loopCount++
	if loopCount < repetitions {
		executeQuery(query, loopCount, nil)
	}
	if wg != nil {
		wg.Done()
	}
}

func addExecutionReport(queryName string, concurrency int, stats *Stats) {
	accumulated := stats.Accumulated.String()
	average := strconv.FormatFloat(stats.Average, 'f', 2, 64)
	successCount := strconv.FormatInt(int64(stats.SuccessCount), 10)
	reps := strconv.Itoa(repetitions)
	concurrent := strconv.Itoa(concurrency)
	errorCount := strconv.FormatInt(int64(stats.ErrorCount), 10)
	var line = []string{queryName, concurrent, reps, accumulated, average, successCount, errorCount}
	e := executionFileWriter.Write(line)
	executionFileWriter.Flush()
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
		errorCount++
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
	var groupStats Stats
	qErrorCount = 0
	qSuccessCount = 0
	qDuration = 0
	for i := 0; i < concurrentCalls; i++ {
		wg.Add(1)
		go executeQuery(query, 0, &wg)
	}
	wg.Wait()
	took := time.Since(startingTime)
	d := time.Duration(qDuration)
	groupStats.Accumulated = d
	groupStats.ErrorCount = qErrorCount
	groupStats.SuccessCount = qSuccessCount
	floated := float64(groupStats.SuccessCount)
	groupStats.Average = float64(groupStats.Accumulated/time.Millisecond) / floated
	log.Printf("Couchbase usage: %s   Per Query: %f   Success: %d   Error: %d", groupStats.Accumulated.String(), groupStats.Average, groupStats.SuccessCount, groupStats.ErrorCount)
	addExecutionReport(name, concurrentCalls, &groupStats)
	log.Printf("----Query %s[%d]: Total SDK Time: %s", name, concurrentCalls, took)
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
	executionFile, fileError = os.Create(*executionOutput)
	if fileError != nil {
		log.Print("Error creating output file (execution)")
		log.Panic(fileError)
	}
	executionFileWriter = csv.NewWriter(executionFile)
	explainFile, fileError = os.Create(*explainOutput)
	phase("Benchmarking queries")
	for _, stmt := range executionConfig.Queries {
		println("\n")
		for _, ndx := range stmt.Indexes {
			createIndex(ndx.Name, ndx.Definition)
		}
		explainIt(stmt.Query)
		for _, v := range executionConfig.Concurrency {
			time.Sleep(*pause)
			testQuery(stmt.Query, stmt.Name, v)
		}
		time.Sleep(*pause * 3)
		for _, ndx := range stmt.Indexes {
			if ndx.DropOnFinish == true {
				dropIndex(ndx.Name)
			}
		}
	}
	phase("Removing common indexes")
	for _, v := range executionConfig.Indexes {
		if v.DropOnFinish == true {
			dropIndex(v.Name)
		}
	}
	defer executionFile.Close()
	defer explainFile.Close()
	log.Printf("Total errors found: %d", errorCount)
	log.Printf("Report %s is available", *executionOutput)
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
		println("\nN1QL Benchmark\n")
		flag.PrintDefaults()
		log.Print("Fido")
	} else {
		if *maxProcs > runtime.NumCPU() {
			*maxProcs = runtime.NumCPU()
		}
		log.Printf("Concurrency %d out of %d CPUs will be used", *maxProcs, runtime.NumCPU())
		runtime.GOMAXPROCS(*maxProcs)
		parseConfig()
		startExecutionPlan()
	}
}
