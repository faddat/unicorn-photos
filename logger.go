package main

import (
	"log"
	"os"
)

// Basic logger, can be replaced with a more advanced one like zerolog, zap, or logrus
// if structured logging or different levels beyond Printf are needed.
var logger = log.New(os.Stdout, "[unicorn-photos] ", log.LstdFlags|log.Lmicroseconds)

// TODO: Implement SetupLogger(level string) if more advanced levels (debug, warn, error) are needed
// based on config.LogLevel.
