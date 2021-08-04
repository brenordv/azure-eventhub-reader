package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// ToString converts a Message to string.
//
// Parameters:
//  None
//
// Receiver:
//  Instance of Message.
//
// Returns:
//  string representation of a Message.
func (m *Message) ToString() string {
	str := fmt.Sprintf(`---| DETAILS      |-----------------------------------------------------
id: %s
added to queue at: %s
event sequence number: %s
event offset: %s
Message processed at: %s
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

// ParseCommandLine parses the command line.
// Will panic in case of failure.
//
// Parameters:
//  None.
//
// Returns:
//  string containing the command/verb and another with the configuration file that must be used.
func ParseCommandLine() (string, string) {
	generalCmd := flag.NewFlagSet("general", flag.ExitOnError)
	readCmdPtr := generalCmd.String("Config", defaultConfigFile, "Which Config file to use.")

	if len(os.Args) < 2 {
		generalCmd.Usage = func() { // [1]
			_, err := fmt.Fprintf(flag.CommandLine.Output(), "usage: %s read|export2file [-Config=<Config file>]\n", os.Args[0])
			HandleError("Error printing command line usage", err, true)
			generalCmd.PrintDefaults()
		}
		generalCmd.Usage()
		exitCode = 0
		runtime.Goexit()
	}

	verb := os.Args[1]
	var err error
	var configFile string

	if verb != "read" && verb != "export2file" && verb != "write" {
		flag.PrintDefaults()
		exitCode = 0
		runtime.Goexit()
	}

	err = generalCmd.Parse(os.Args[2:])
	HandleError(fmt.Sprintf("Failed to parse '%s' command line", verb), err, true)
	configFile = *readCmdPtr

	if configFile == defaultConfigFile {
		configFile = filepath.Join(GetAppDir(), configFile)
	}

	return verb, configFile
}

// Serialize converts a Message to byte[] so it can be saved to badgerDb.
// Will panic in case of failure.
//
// Parameters:
//  None.
//
// Receiver:
//  Instance of Message.
//
// Returns:
//  slice of bytes representing a instance of Message.
func (m *Message) Serialize() []byte {
	var res bytes.Buffer
	encoder := gob.NewEncoder(&res)
	err := encoder.Encode(m)
	HandleError("Failed to Serialize Message.", err, true)

	return res.Bytes()
}

// Deserialize Message object returned from badgerDb
// Will panic in case of failure.
//
// Parameters:
//  data: instance of Message in a slice of bytes.
//
// Returns:
//  Deserialized instance of Message.
func Deserialize(data []byte) *Message {
	var m Message

	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&m)

	HandleError("Failed to Deserialize Message.", err, true)

	return &m
}
