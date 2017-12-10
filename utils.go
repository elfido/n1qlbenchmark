package main

import (
	"log"
)

func panicIt(err error, message string) {
	if err != nil {
		log.Print(err)
		log.Panic(message)
	}
}
