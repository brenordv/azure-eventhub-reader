package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"
)

// WrapUpExecution will wrap up anything that needs closing and also print a final Message.
// Will panic in case of failure.
//
// Parameters:
//  None
//
// Returns:
//  Nothing.
func WrapUpExecution() {
	if pBar != nil {
		err := pBar.Close()
		HandleError("Failed to close progress bar.", err, false)
	}
	log.Println(fmt.Sprintf("\nAll done! (elapsed time: %s)", time.Since(start)))
	os.Exit(exitCode)
}

// PrepareToRun do all the preparations to execute an operation.
// if this method fails, execution cannot continue.
// Will panic in case of failure.
//
// Parameters:
//  None
//
// Returns:
//  Nothing
func PrepareToRun() string {
	exitCode = 1
	op, cfgFile := ParseCommandLine()
	op = strings.ToLower(op)
	if !FileOrDirExists(cfgFile) {
		HandleError(
			"Config file validation failed!",
			fmt.Errorf("file '%s' does not exist or is unaccessible", cfgFile),
			true)
	}

	LoadConfig(cfgFile)
	ValidateRunConfiguration(cfgFile, op)
	return op
}

// HandleError just a wrapper to handle errors in a neat, lazy manner.
//
// Parameters:
//  customMsg: this message will be logger alongside the error
//  err: error object that will be logger.
//  shouldPanic: if true, will panic. Otherwise, will just signal the application to exit.
//
// Returns:
//  Nothing
func HandleError(customMsg string, err error, shouldPanic bool) {
	if err == nil {
		return
	}
	errMsg := fmt.Sprintf("[ERROR] %s. Details: %s", customMsg, err)
	log.Println(errMsg)
	if !shouldPanic {
		log.Println(err)
		exitCode = 0
		runtime.Goexit()
	}
	panic(err)
}

// WaitForUserInterruption will wait for the user to stop execution to continue.
// this means that any code after this method will only run after the ser presses Ctrl+C or something like that.
// Will panic in case of failure.
//
// Parameters:
//  None
//
// Returns:
//  Nothing.
func WaitForUserInterruption() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
	WrapUpExecution()
}
