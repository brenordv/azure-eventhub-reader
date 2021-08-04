package main

import (
	"context"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/dgraph-io/badger/v3"
	"github.com/schollz/progressbar/v3"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func main() {
	start = time.Now()
	fmt.Println(fmt.Sprintf("%sAzure Eventhub%s tools. (v: %s)\n", colorBlue, colorReset, version))
	defer WrapUpExecution()
	operation := PrepareToRun()

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

	case "write":
		log.Println(fmt.Sprintf("Preparing to send all files in outbound folder '%s' as messages to Eventhub...",
			currentConfig.OutboundFolder))
		sendToEventhub()
	default:
		log.Println(fmt.Sprintf("Operation '%s' is not supported.", operation))
	}
}

// readEventHubMessages starts the routines to read from eventhub and save it to badgerDb.
func readEventHubMessages() {
	if readToFile {
		PrintReadAndSafeToDiskPerfWarning()
		dataDumpDir = GetDataDumpDir()
	}
	messageChannel = make(chan Message)
	pBar = progressbar.Default(
		-1,
		"Reading messages...",
	)

	go StartReceivingMessages(currentConfig.EventhubConnectionString, currentConfig.EntityPath)
	go ProcessMessage()

	WaitForUserInterruption()

	CloseConnection()

}

// exportToFile will read the database and export any file.
func exportToFile() {
	dataDumpDir = GetDataDumpDir()
	pBar = progressbar.Default(
		-1,
		"Exporting messages to file...",
	)
	db := OpenConnection()

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true

		iter := txn.NewIterator(opts)
		defer iter.Close()
		go WaitForUserInterruption()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			{
				_ = pBar.Add(1)
				dbRow := iter.Item()
				err := dbRow.Value(func(val []byte) error {
					msg := Deserialize(val)
					msgPath := filepath.Join(GetDataDumpDirBasedOnTime(msg.ProcessedAt), msg.DumpFilename)
					if FileOrDirExists(msgPath) {
						return nil
					}

					DumpMessage(*msg, msgPath)
					return nil
				})

				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	HandleError("Error iterating through database", err, true)

	CloseConnection()
}

// sendToEventhub will watch a folder and send every new file as a Message to eventhub.
func sendToEventhub() {
	ctx, hub := GetEventHubClient(currentConfig.EventhubConnectionString, currentConfig.EntityPath)
	defer func(hub *eventhub.Hub, ctx context.Context) {
		err := hub.Close(ctx)
		if err != nil {
			HandleError("Failed to close eventhub client.", err, true)
		}
	}(hub, ctx)

	var wg sync.WaitGroup
	pending, pendingCount := ListFiles(currentConfig.OutboundFolder)
	pBar = progressbar.Default(
		int64(pendingCount),
		"Sending files...",
	)

	for _, f := range pending {
		wg.Add(1)
		go func(f string, hub *eventhub.Hub, ctx context.Context, wg *sync.WaitGroup) {
			defer wg.Done()
			fContent := ReadTextFile(f)
			err := hub.Send(ctx, eventhub.NewEventFromString(fContent))
			_ = pBar.Add(1)
			HandleError(fmt.Sprintf("Failed to send file '%s' to eventhub.", f),
				err, true)

			var fi os.FileInfo
			fi, err = os.Stat(f)
			HandleError(fmt.Sprintf("Failed to get status of file '%s'.", f), err, true)

			if currentConfig.DontMoveSentFiles {
				return
			}

			MoveFile(f,
				filepath.Join(currentConfig.OutboundFolderSent,
					fmt.Sprintf("%s--%s", time.Now().Format("2006-01-02T15-04-05.000000000"), fi.Name())))
		}(f, hub, ctx, &wg)
	}

	wg.Wait()
}
