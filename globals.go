package main

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/schollz/progressbar/v3"
	"time"
)

// Message is the representation of metadata for a received azure eventhub Message.
type Message struct {
	EventId        string
	QueuedTime     time.Time
	EventSeqNumber *int64
	EventOffset    *int64
	DumpFilename   string
	ProcessedAt    time.Time
	ElapsedTime    string
	MsgData        string
}

// Config is the configuration read from the file passed via command line argument.
type Config struct {
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
	OutboundFolder             string `json:"outboundFolder"`
	OutboundFolderSent         string `json:"outboundFolderSent"`
	DontMoveSentFiles          bool   `json:"dontMoveSentFiles"`
}

// application constants
const (
	version                = "1.1.0.1"
	badgerBase             = ".\\.appdata"
	badgerDir              = ".\\.appdata\\dir"
	badgerValueDir         = ".\\.appdata\\valueDir"
	messageDumpDir         = ".\\.data-dump\\eventhub"
	outboundFolder         = ".\\.outbound"
	outboundFolderSent     = ".\\.outbound\\.sent"
	readToFile             = false
	colorBlue              = "\033[34m"
	colorReset             = "\033[0m"
	defaultConfigFile      = ".\\default.conf.json"
	badgerValueLogFileSize = 10485760
)

// global variables
var messageChannel chan Message
var pBar *progressbar.ProgressBar
var badgerConnection *badger.DB
var dataDumpDir string
var appDir string
var currentConfig Config
var exitCode int
var start time.Time
