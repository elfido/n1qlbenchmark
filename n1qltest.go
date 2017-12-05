package main

import (
	"encoding/json"
	"flag"
	"github.com/couchbase/gocb"
	"io/ioutil"
	"log"
	"time"
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
var executionConfig Configfile
var bucket *gocb.Bucket

func createIndex(name string, definition []string) {
	log.Printf("Creating index %s", name)
	err := bucket.Manager(executionConfig.User, executionConfig.Password).CreateIndex(name, definition, true, false)
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

func startExecutionPlan() {
	connectionStr := "couchbase://" + executionConfig.Cbhost
	log.Printf("Connecting to bucket %s in cluster %s", executionConfig.Bucket, connectionStr)
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
		createPrimaryIndex()
	}
	for _, v := range executionConfig.Indexes {
		createIndex(v.Name, v.Definition)
	}
	for _, v := range executionConfig.Concurrency {
		log.Printf("Testing with concurrency %d", v)
	}
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
		parseConfig()
		startExecutionPlan()
	}
}
