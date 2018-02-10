package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb"
)

const VERSION = "1.0.0"

type queryStats struct {
	Accumulated  time.Duration
	SuccessCount int32
	ErrorCount   int32
	Average      float64
}

/*
Query defines the Queries that will be used during the benchmark
*/
type Query struct {
	Name    string  `json:"name"`
	Query   string  `json:"query"`
	Indexes []Index `json:"indexes"`
}

/*
Index defines the name and index fields required by the benchmark
*/
type Index struct {
	Name         string   `json:"name"`
	Definition   []string `json:"definition"`
	DropOnFinish bool     `json:"dropOnFinish"`
}

type report struct {
	Name        string
	Concurrency int
	Stats       *queryStats
}

type explainReport struct {
	Query string      `json:"query"`
	Plan  interface{} `json:"plan"`
}

var configuration = flag.String("i", "config.json", "Path to the configuration file")
var executionOutput = flag.String("o", "report-execution.csv", "Output file")
var explainOutput = flag.String("p", "explain.json", "Query explain plan file")
var showHelp = flag.Bool("?", false, "Shows CLI help :)")
var queryTimeout = flag.Duration("t", (30 * time.Second), "Query timeout")
var pause = flag.Duration("pause", 2*time.Second, "Pause between queries, this will help the cluster to recover")
var maxProcs = flag.Int("c", 2, "Max concurrency")
var executionConfig Configfile
var bucket *gocb.Bucket
var phaseCount = 0
var errorCount = 0
var repetitions = 1
var qSuccessCount int32
var qErrorCount int32
var qDuration int64
var finalReport []report
var plans []explainReport
var currentIndex = 0

// Output files
// var explainFile *os.File
var explainJsonFile *os.File
var executionFile *os.File
var executionFileWriter *csv.Writer

func createIndex(name string, definition []string) {
	log.Printf("Creating index %s", name)
	err := bucket.Manager(executionConfig.User, executionConfig.Password).CreateIndex(name, definition, true, false)
	genericEHandler(err)
}

func dropIndex(name string) {
	log.Printf("Removing index %s", name)
	err := bucket.Manager(executionConfig.User, executionConfig.Password).DropIndex(name, true)
	genericEHandler(err)
}

func createPrimaryIndex() {
	log.Print("Creating primary index")
	err := bucket.Manager(executionConfig.User, executionConfig.Password).CreatePrimaryIndex("primary", false, false)
	genericEHandler(err)
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

func addExecutionReport(queryName string, concurrency int, stats *queryStats) {
	accumulated := stats.Accumulated.String()
	average := strconv.FormatFloat(stats.Average, 'f', 2, 64)
	successCount := strconv.FormatInt(int64(stats.SuccessCount), 10)
	reps := strconv.Itoa(repetitions)
	concurrent := strconv.Itoa(concurrency)
	errorCount := strconv.FormatInt(int64(stats.ErrorCount), 10)
	var line = []string{queryName, concurrent, reps, accumulated, average, successCount, errorCount}
	e := executionFileWriter.Write(line)
	executionFileWriter.Flush()
	genericEHandler(e)
}

func addExplainReport(query string, report string) {
	var content interface{}
	bytes := []byte(report)
	err := json.Unmarshal(bytes, &content)
	if err == nil {
		plan := explainReport{
			Query: query,
			Plan:  content,
		}
		plans[currentIndex] = plan
	}
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
	var groupStats queryStats
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
	var rep report
	rep.Name = name
	rep.Stats = &groupStats
	rep.Concurrency = concurrentCalls
	finalReport[currentIndex] = rep
	log.Printf("----Query %s[%d]: Total SDK Time: %s", name, concurrentCalls, took)
}

func startConnection() {
	connectionStr := "couchbase://" + executionConfig.Cbhost
	log.Printf("Connecting to bucket %s in cluster %s and query timeout is %s", executionConfig.Bucket, connectionStr, *queryTimeout)
	cluster, err := gocb.Connect(connectionStr)
	panicIt(err, "Cannot connect to cluster")
	cluster.SetConnectTimeout(2 * time.Second)
	if executionConfig.User != "" {
		log.Printf("Authenticating user %s", executionConfig.User)
		cluster.Authenticate(gocb.PasswordAuthenticator{
			Username: executionConfig.User,
			Password: executionConfig.Password,
		})
	}
	cbBucket, berr := cluster.OpenBucket(executionConfig.Bucket, executionConfig.Password)
	panicIt(berr, "Cannot connect to bucket")
	bucket = cbBucket
}

func createCommonIndexes() {
	if executionConfig.CreatePrimary == true {
		phase("Creating primary index")
		createPrimaryIndex()
	}
	phase("Creating common indexes")
	for _, v := range executionConfig.Indexes {
		createIndex(v.Name, v.Definition)
	}
}

func removeCommonIndexes() {
	phase("Removing common indexes")
	for _, v := range executionConfig.Indexes {
		if v.DropOnFinish == true {
			dropIndex(v.Name)
		}
	}
}

func startBenchmark() {
	phase("Benchmarking queries")
	for _, stmt := range executionConfig.Queries {
		println("")
		for _, ndx := range stmt.Indexes {
			createIndex(ndx.Name, ndx.Definition)
		}
		explainIt(stmt.Query)
		for _, v := range executionConfig.Concurrency {
			time.Sleep(*pause)
			testQuery(stmt.Query, stmt.Name, v)
			currentIndex++
		}
		time.Sleep(*pause * 3)
		for _, ndx := range stmt.Indexes {
			if ndx.DropOnFinish == true {
				dropIndex(ndx.Name)
			}
		}
	}
}

func closeExplainReport() {
	txt, err := json.MarshalIndent(plans, "", "\t")
	if err == nil {
		explainJsonFile.WriteString(string(txt))
	}
	explainJsonFile.Close()
}

func startExecutionPlan() {
	startConnection()
	createCommonIndexes()
	phase("Creating output file")
	var fileError error
	executionFile, fileError = os.Create(*executionOutput)
	panicIt(fileError, "Error creating output file (execution)")
	executionFileWriter = csv.NewWriter(executionFile)
	explainJsonFile, fileError = os.Create(*explainOutput)
	totalRecords := len(executionConfig.Queries) * len(executionConfig.Concurrency)
	finalReport = make([]report, totalRecords)
	plans = make([]explainReport, len(executionConfig.Queries))
	startBenchmark()
	removeCommonIndexes()
	defer executionFile.Close()
	defer closeExplainReport()
	log.Printf("\n\nTotal errors found: %d", errorCount)
	log.Printf("Report %s is available\n\n", *executionOutput)
	printSummary()
}

func main() {
	flag.Parse()
	if *showHelp == true {
		println("\nN1QL Benchmark " + VERSION)
		flag.PrintDefaults()
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
