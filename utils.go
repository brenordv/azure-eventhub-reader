package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"
)

// GetAppDir returns the path of this application. if it's not loaded yet, will save it to a global variable.
// Will panic if it cannot get the application path.
//
// Parameters:
//  None.
//
// Returns:
//  String containing the path of the application.
func GetAppDir() string {
	if appDir == "" {
		execPath, err := os.Executable()
		HandleError("Failed to get application directory.", err, true)
		appDir = path.Dir(execPath)
	}
	return appDir
}

// GetDataDumpDir returns a string with a valid path to use when saving database content to disk.
// Will panic in case of failure.
//
// Parameters:
//  None.
//
// Returns:
//  String containing the directory used to dump data with sub-folder to organize messages by day.
func GetDataDumpDir() string {
	return GetDataDumpDirBasedOnTime(time.Now())
}

// GetDataDumpDirBasedOnTime returns a string with a valid path to use when saving database content to disk.
// Will panic in case of failure.
//
// Parameters:
//  ts: time that will be used as base to create the data dump folder.
//
// Returns:
//  String containing the directory used to dump data with sub-folder to organize messages by day.
func GetDataDumpDirBasedOnTime(ts time.Time) string {
	dataDumpDir = filepath.Join(currentConfig.MessageDumpDir, ts.Format("2006-01-02"))
	EnsureDirExists(dataDumpDir)
	return dataDumpDir
}

// GetDumpMsgFilename generates a filename based on current time and the eventId.
// Will panic in case of failure.
//
// Parameters:
//  eventId: id of the eventhub message.
//
// Returns:
//  filename that will be used to dump an eventhub message.
func GetDumpMsgFilename(eventId string) string {
	return filepath.Join(
		dataDumpDir,
		fmt.Sprintf("%s--%s.txt",
			time.Now().Format("2006-01-02T15-04-05.00"), eventId))
}

// LoadConfig loads execution configuration from file to the global variable.
// Will panic in case of failure.
//
// Parameters:
//  f: filename that will be loaded
//
// Returns:
//  Nothing.
func LoadConfig(f string) {
	file, err := os.Open(f)
	HandleError(fmt.Sprintf("Failed to open Config file: %s", f), err, true)

	jsonParser := json.NewDecoder(file)
	err = jsonParser.Decode(&currentConfig)
	HandleError(fmt.Sprintf("Failed to read Config file: %s", f), err, true)
}

// PrintReadAndSafeToDiskPerfWarning simply prints out a warning message if the user decides
// to read messages AND write them to disk at the same time.
//
// Parameters:
//  None.
//
// Returns:
//  Nothing.
func PrintReadAndSafeToDiskPerfWarning() {
	log.Println("----| WARNING | ----------------------------------")
	log.Println("Reading to file drastically SLOWS things down.")
	log.Println("Benchmarks:")
	log.Println("Save to Database only:		~300 messages/sec")
	log.Println("Save to Database + file:	~25 messages/sec")
	log.Println("--------------------------------------------------")
}
