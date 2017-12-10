package main

import (
	"fmt"
	"log"
)

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
