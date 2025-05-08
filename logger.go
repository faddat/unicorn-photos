package main

import (
	"log"
	"os"
)

var logger = log.New(os.Stdout, "[unicorn-photos] ", log.LstdFlags)
