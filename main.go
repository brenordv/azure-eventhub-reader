package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/dgraph-io/badger/v3"
	"github.com/schollz/progressbar/v3"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// message is the representation of metadata for a received azure eventhub message.
type message struct {
	EventId        string
	QueuedTime     time.Time
	EventSeqNumber *int64
	EventOffset    *int64
	DumpFilename   string
	ProcessedAt    time.Time
	ElapsedTime    string
	MsgData        string
}

// config is the configuration read from the file passed via command line argument.
type config struct {
	MessageDumpDir             string `json:"messageDumpDir"`
	BadgerBase                 string `json:"badgerBase"`
	BadgerDir                  string `json:"badgerDir"`
	BadgerValueDir             string `json:"badgerValueDir"`
	BadgerValueLogFileSize     int64  `json:"badgerValueLogFileSize"`
	BadgerSkipCompactL0OnClose bool   `json:"badgerSkipCompactL0OnClose"`
	BadgerVerbose              bool   `json:"badgerVerbose"`
	EventhubConnectionString   string `json:"eventhubConnString"`
	EntityPath                 string `json:"entityPath"`
	ReadToFile                 bool   `json:"readToFile"`
	ConsumerGroup              string `json:"consumerGroup"`
	DumpOnlyMessageData        bool   `json:"dumpOnlyMessageData"`
	Env                        string `json:"env"`
}

// application constants
const (
	version                = "1.0.1.0"
	badgerBase             = ".\\.appdata"
	badgerDir              = ".\\.appdata\\dir"
	badgerValueDir         = ".\\.appdata\\valueDir"
	messageDumpDir         = ".\\.data-dump\\eventhub"
	readToFile             = false
	colorBlue              = "\033[34m"
	colorReset             = "\033[0m"
	defaultConfigFile      = ".\\default.conf.json"
	badgerValueLogFileSize = 10485760
)

// global variables
var messageChannel chan message
var pBar *progressbar.ProgressBar
var badgerConnection *badger.DB
var dataDumpDir string
var appDir string
var currentConfig config
var exitCode int
var start time.Time

func main() {
	start = time.Now()
	fmt.Println(fmt.Sprintf("%sAzure Eventhub%s tools. (v: %s)\n", colorBlue, colorReset, version))
	defer wrapUpExecution()
	operation := prepareToRun()

	switch operation {
	case "read":
		log.Println(
			fmt.Sprintf("Preparing to continuosly read messages from eventhub on entity '%s'...",
				currentConfig.EntityPath))
		readEventHubMessages()
		break

	case "export2file":
		log.Println("Preparing to export all rows to file...")
		exportToFile()
		break

	default:
		log.Println(fmt.Sprintf("Operation '%s' is not supported.", operation))
	}
}

// wrapUpExecution will wrap up anything that needs closing and also print a final message.
func wrapUpExecution() {
	if pBar != nil {
		err := pBar.Close()
		handleError("Failed to close progress bar.", err, false)
	}
	fmt.Println(fmt.Sprintf("\nAll done! (elapsed time: %s)", time.Since(start)))
	os.Exit(exitCode)
}

// prepareToRun do all the preparations to execute an operation.
// if this method fails, execution cannot continue.
func prepareToRun() string {
	exitCode = 1
	op, cfgFile := parseCommandLine()
	op = strings.ToLower(op)
	err := validateCfgFile(cfgFile)
	handleError("Config file validation failed!", err, true)
	loadConfig(cfgFile, op)
	return op
}

// loadConfig loads execution configuration from file.
func loadConfig(f string, op string) {
	errMsg := fmt.Sprintf("Configuration file '%s' is invalid.", f)
	file, err := os.Open(f)
	handleError(fmt.Sprintf("Failed to open config file: %s", f), err, true)

	jsonParser := json.NewDecoder(file)
	err = jsonParser.Decode(&currentConfig)
	handleError(fmt.Sprintf("Failed to read config file: %s", f), err, true)

	if currentConfig.Env == "" {
		handleError(errMsg,
			errors.New("key 'env' is missing or empty"),
			true)
	}

	if currentConfig.EntityPath == "" {
		handleError(errMsg,
			errors.New("key 'entityPath' is missing or empty"),
			true)
	}

	if currentConfig.ConsumerGroup == "" {
		handleError(errMsg,
			errors.New("key 'consumerGroup' is missing and I'll not assume $default"),
			true)
	}

	if currentConfig.BadgerValueLogFileSize == 0 {
		currentConfig.BadgerValueLogFileSize = badgerValueLogFileSize
	}

	bDir := getAppDir()
	if currentConfig.BadgerBase == "" {
		currentConfig.BadgerBase = filepath.Join(bDir, badgerBase)
	}

	if currentConfig.BadgerDir == "" {
		currentConfig.BadgerDir = filepath.Join(filepath.Join(bDir, badgerDir), currentConfig.Env)
	}

	if currentConfig.BadgerValueDir == "" {
		currentConfig.BadgerValueDir = filepath.Join(filepath.Join(bDir, badgerValueDir), currentConfig.Env)
	}

	if currentConfig.MessageDumpDir == "" {
		currentConfig.MessageDumpDir = filepath.Join(bDir, messageDumpDir)
	}

	ensureDirExists(currentConfig.BadgerBase)
	ensureDirExists(currentConfig.BadgerDir)
	ensureDirExists(currentConfig.BadgerValueDir)
	if op == "export2file" || currentConfig.ReadToFile {
		ensureDirExists(currentConfig.MessageDumpDir)
	}
}

// validateCfgFile checks if the configuration file exist.
func validateCfgFile(file string) error {
	if fileOrDirExists(file) {
		return nil
	}

	return fmt.Errorf("file '%s' does not exist or is unaccessible", file)
}

// readEventHubMessages starts the routines to read from eventhub and save it to badgerDb.
func readEventHubMessages() {
	if readToFile {
		log.Println("----| WARNING | ----------------------------------")
		log.Println("Reading to file drastically SLOWS things down.")
		log.Println("Benchmarks:")
		log.Println("Save to Database only:		~300 messages/sec")
		log.Println("Save to Database + file:	~25 messages/sec")
		log.Println("--------------------------------------------------")
		dataDumpDir = getDataDumpDir()
	}
	messageChannel = make(chan message)
	pBar = progressbar.Default(
		-1,
		"Reading messages...",
	)

	go startReceivingMessages(currentConfig.EventhubConnectionString, currentConfig.EntityPath)
	go processMessage()

	waitForUserInterruption()

	closeThePub()

}

// exportToFile will read the database and export any file.
func exportToFile() {
	dataDumpDir = getDataDumpDir()
	pBar = progressbar.Default(
		-1,
		"Exporting messages to file...",
	)
	db := openConnection()

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true

		iter := txn.NewIterator(opts)
		defer iter.Close()
		go waitForUserInterruption()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			{
				_ = pBar.Add(1)
				dbRow := iter.Item()
				err := dbRow.Value(func(val []byte) error {
					msg := deserialize(val)
					msgPath := filepath.Join(getDataDumpDirBasedOnTime(msg.ProcessedAt), msg.DumpFilename)
					if fileOrDirExists(msgPath) {
						return nil
					}

					dumpMessage(*msg, msgPath)
					return nil
				})

				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	handleError("Error iterating through database", err, true)

	closeThePub()
}

// waitForUserInterruption will wait for the user to stop execution to continue.
// this means that any code after this method will only run after the ser presses Ctrl+C or something like that.
func waitForUserInterruption() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
	wrapUpExecution()
}

// StartReceivingMessages will create an instance of Eventhub Consumer and wait for messages.
func startReceivingMessages(connectionString string, entityPath string) {
	ctx := context.Background()

	if !strings.Contains(connectionString, ";EntityPath=") {
		connectionString = fmt.Sprintf("%s;EntityPath=%s", connectionString, entityPath)
	}

	hub, err := eventhub.NewHubFromConnectionString(connectionString)
	if err != nil {
		panic(err)
	}

	_, err = hub.Receive(ctx, "0", onMsgReceived, eventhub.ReceiveWithConsumerGroup(currentConfig.ConsumerGroup))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

// OnMsgReceived is the handler for received messages on eventhub.
func onMsgReceived(_ context.Context, event *eventhub.Event) error {
	checkpoint := message{
		EventId:        event.ID,
		QueuedTime:     *event.SystemProperties.EnqueuedTime,
		EventSeqNumber: event.SystemProperties.SequenceNumber,
		EventOffset:    event.SystemProperties.Offset,
		ProcessedAt:    time.Now(),
		MsgData:        string(event.Data),
		DumpFilename:   getDumpMsgFilename(event.ID),
	}

	_ = pBar.Add(1)

	messageChannel <- checkpoint
	return nil
}

// processMessage is a routine to process received messages.
func processMessage() {
	db := openConnection()

	for {
		msg, channelOpen := <-messageChannel
		if !channelOpen {
			break
		}

		if !stillHaveConnection(db) {
			log.Println("Connection closed. Cancelling read operation...")
			return
		}

		dbErr := db.Update(func(txn *badger.Txn) error {
			if !stillHaveConnection(db) {
				return nil
			}
			if _, err := txn.Get([]byte(msg.EventId)); err == badger.ErrKeyNotFound {
				if !stillHaveConnection(db) {
					return nil
				}

				msg.ElapsedTime = fmt.Sprintf("%s", time.Since(msg.ProcessedAt))
				if currentConfig.ReadToFile {
					dumpMessage(msg, filepath.Join(getDataDumpDirBasedOnTime(msg.ProcessedAt), msg.DumpFilename))
				}

				err = txn.Set([]byte(msg.EventId), msg.serialize())

				return err
			}
			return nil
		})

		handleError("Failed to process received message.", dbErr, true)
	}
}

// stillHaveConnection helps to avoid panic errors when handling abrupt runtime interruptions (ctrl+c or stopping ide
// run). This has to be called everytime before a database operation is performed.
// Simply checks if the DB object is not null and if the connection is not closed.
func stillHaveConnection(db *badger.DB) bool {
	return db != nil && !db.IsClosed()
}

// openConnection opens a connection with badgerDb.
func openConnection() *badger.DB {
	if badgerConnection != nil && !badgerConnection.IsClosed() {
		return badgerConnection
	}

	opts := badger.DefaultOptions(currentConfig.BadgerBase)
	opts.Dir = currentConfig.BadgerDir
	opts.ValueDir = currentConfig.BadgerValueDir
	opts.CompactL0OnClose = !currentConfig.BadgerSkipCompactL0OnClose
	opts.ValueLogFileSize = currentConfig.BadgerValueLogFileSize

	if !currentConfig.BadgerVerbose {
		opts.Logger = nil
	}

	var err error
	badgerConnection, err = badger.Open(opts)
	handleError("To open badger database.", err, true)
	return badgerConnection
}

// closeThePub closes the connection to badgerDb, if it's open.
func closeThePub() {
	if badgerConnection == nil || badgerConnection.IsClosed() {
		return
	}

	err := badgerConnection.Close()
	handleError("Failed to close badger connection.", err, true)
}

// serialize converts a message to byte[] so it can be saved to badgerDb.
func (m *message) serialize() []byte {
	var res bytes.Buffer
	encoder := gob.NewEncoder(&res)
	err := encoder.Encode(m)
	handleError("Failed to serialize message.", err, true)

	return res.Bytes()
}

// deserialize message object returned from badgerDb
func deserialize(data []byte) *message {
	var m message

	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&m)

	handleError("Failed to deserialize message.", err, true)

	return &m
}

// getDumpMsgFilename generates a filename based on current time and the eventId.
func getDumpMsgFilename(eventId string) string {
	return filepath.Join(
		dataDumpDir,
		fmt.Sprintf("%s--%s.txt",
			time.Now().Format("2006-01-02T15-04-05.00"), eventId))
}

// dumpMessage creates a text file with the message received.
func dumpMessage(checkpoint message, path string) {
	file, err := os.Create(path)
	if err != nil {
		handleError(fmt.Sprintf("Failed to create file '%s'.", path), err, true)
	} else {
		defer func() {
			if err := file.Close(); err != nil {
				handleError(fmt.Sprintf("Failed to close file '%s'.", path), err, true)
			}
		}()
		var content string
		if currentConfig.DumpOnlyMessageData {
			content = checkpoint.MsgData
		} else {
			content = checkpoint.toString()
		}

		_, err = io.WriteString(file, content)
		if err != nil {
			handleError(fmt.Sprintf("Failed to write to file '%s'.", path), err, true)
		} else {
			defer func() {
				if err := file.Sync(); err != nil {
					handleError(fmt.Sprintf("Failed to flush file '%s' to disk.", path), err, true)
				}
			}()
		}
	}
}

// toString converts a message to string.
func (m *message) toString() string {
	str := fmt.Sprintf(`---| DETAILS      |-----------------------------------------------------
id: %s
added to queue at: %s
event sequence number: %s
event offset: %s
message processed at: %s
processing elapsed time: %s

---| MESSAGE BODY |-----------------------------------------------------
%s`,
		m.EventId,
		m.QueuedTime.Format(time.RFC3339Nano),
		strconv.FormatInt(*m.EventSeqNumber, 10),
		strconv.FormatInt(*m.EventOffset, 10),
		m.ProcessedAt.Format(time.RFC3339Nano),
		m.ElapsedTime,
		m.MsgData)

	return str
}

// ensureDirExists if the path does not exist, tries to create it.
func ensureDirExists(path string) {
	if fileOrDirExists(path) {
		return
	}
	err := os.MkdirAll(path, os.ModePerm)
	handleError(fmt.Sprintf("Failed to create path '%s'.", path), err, true)
}

// fileOrDirExists returns whether the given file or directory exists.
func fileOrDirExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return !os.IsNotExist(err)

}

// handleError just a wrapper to handle errors in a neat, lazy manner.
func handleError(customMsg string, err error, shouldPanic bool) {
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

// getDataDumpDir returns a string with a valid path to use when saving database content to disk.
func getDataDumpDir() string {
	return getDataDumpDirBasedOnTime(time.Now())
}

// getDataDumpDirBasedOnTime returns a string with a valid path to use when saving database content to disk.
func getDataDumpDirBasedOnTime(ts time.Time) string {
	dataDumpDir = filepath.Join(currentConfig.MessageDumpDir, ts.Format("2006-01-02"))
	ensureDirExists(dataDumpDir)
	return dataDumpDir
}

// parseCommandLine parses the command line
func parseCommandLine() (string, string) {
	generalCmd := flag.NewFlagSet("general", flag.ExitOnError)
	readCmdPtr := generalCmd.String("config", defaultConfigFile, "Which config file to use.")

	if len(os.Args) < 2 {
		generalCmd.Usage = func() { // [1]
			_, err := fmt.Fprintf(flag.CommandLine.Output(), "usage: %s read|export2file [-config=<config file>]\n", os.Args[0])
			handleError("Error printing command line usage", err, true)
			generalCmd.PrintDefaults()
		}
		generalCmd.Usage()
		exitCode = 0
		runtime.Goexit()
	}

	verb := os.Args[1]
	var err error
	var configFile string

	if verb != "read" && verb != "export2file" {
		flag.PrintDefaults()
		exitCode = 0
		runtime.Goexit()
	}

	err = generalCmd.Parse(os.Args[2:])
	handleError(fmt.Sprintf("Failed to parse '%s' command line", verb), err, true)
	configFile = *readCmdPtr

	if configFile == defaultConfigFile {
		configFile = filepath.Join(getAppDir(), configFile)
	}

	return verb, configFile
}

// getAppDir returns the path of this application. if it's not loaded yet, will save it to a global variable.
func getAppDir() string {
	if appDir == "" {
		execPath, err := os.Executable()
		handleError("Failed to get application directory.", err, true)
		appDir = path.Dir(execPath)
	}
	return appDir
}
