package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

/*
ConfigFile defines the structure of the configuration JSON file used as input for the benchmark plan
*/
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

func parseConfig() {
	data, err := ioutil.ReadFile(*configuration)
	if err != nil {
		log.Print(err)
		log.Panicf("Cannot open the configuration file %s", *configuration)
	}
	err = json.Unmarshal(data, &executionConfig)
	panicIt(err, "Invalid configuration file or format")
	repetitions = executionConfig.Repetitions
}

func genericEHandler(err error) {
	if err != nil {
		log.Print(err)
	}
}

func phase(message string) {
	phaseCount++
	log.Printf("Phase %d %s", phaseCount, message)
}

func panicIt(err error, message string) {
	if err != nil {
		log.Print(err)
		log.Panic(message)
	}
}

func printSummary() {
	fmt.Printf("\n|%-40s|%-4s|%-5s|%-5s|%-15s|\n", "Query", "Conc", "Suc", "Err", "Avg")
	for _, r := range finalReport {
		fmt.Printf("|%-40s|%4d|%5d|%5d|%15f|\n", r.Name, r.Concurrency, r.Stats.SuccessCount, r.Stats.ErrorCount, r.Stats.Average)
	}
}
