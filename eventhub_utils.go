package main

import (
	"context"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/dgraph-io/badger/v3"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// StartReceivingMessages will create an instance of Eventhub Consumer and wait for messages.
// Will panic in case of failure.
//
// Parameters:
//  connectionString: connection string that will be used to open a connection to Eventhub
//  entityPath: name of the entity path (eventhub) that will be targeted.
//
// Returns:
//  Nothing.
func StartReceivingMessages(connectionString string, entityPath string) {
	var err error
	ctx, hub := GetEventHubClient(connectionString, entityPath)

	_, err = hub.Receive(ctx, "0", OnMsgReceived, eventhub.ReceiveWithConsumerGroup(currentConfig.ConsumerGroup))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

// GetEventHubClient instantiate an Eventhub.
// Will panic in case of failure.
//
// Parameters:
//  connectionString: connection string that will be used to open a connection to Eventhub
//  entityPath: name of the entity path (eventhub) that will be targeted.
//
// Returns:
//  context and eventhub client.
func GetEventHubClient(connectionString string, entityPath string) (context.Context, *eventhub.Hub) {
	ctx := context.Background()

	if !strings.Contains(connectionString, ";EntityPath=") {
		connectionString = fmt.Sprintf("%s;EntityPath=%s", connectionString, entityPath)
	}

	hub, err := eventhub.NewHubFromConnectionString(connectionString)
	HandleError("Failed to create new EventHub client from connection string", err, true)

	info, e := hub.GetRuntimeInformation(ctx)
	HandleError("Failed to get runtime information", e, true)

	log.Printf("Runtime started at '%s', pointing at path '%s' with %d partitions. Available partitions: %s\n",
		info.CreatedAt, info.Path, info.PartitionCount, info.PartitionIDs)

	return ctx, hub
}

// OnMsgReceived is the handler for received messages on eventhub.
//
// Parameters:
//  _: Context. Passed automatically by the eventhub client. Not used, but can't get rid of it.
//  event: pointer to the event containing all the data we need.
//
// Returns:
//  Nothing
func OnMsgReceived(_ context.Context, event *eventhub.Event) error {
	checkpoint := Message{
		EventId:        event.ID,
		QueuedTime:     *event.SystemProperties.EnqueuedTime,
		EventSeqNumber: event.SystemProperties.SequenceNumber,
		EventOffset:    event.SystemProperties.Offset,
		ProcessedAt:    time.Now(),
		MsgData:        string(event.Data),
		DumpFilename:   GetDumpMsgFilename(event.ID),
	}

	_ = pBar.Add(1)

	messageChannel <- checkpoint
	return nil
}

// ProcessMessage is a routine to process received messages.
// Will panic in case of failure.
//
// Parameters:
//  None
//
// Returns:
//  Nothing.
func ProcessMessage() {
	db := OpenConnection()

	for {
		msg, channelOpen := <-messageChannel
		if !channelOpen {
			break
		}

		if !StillHaveConnection(db) {
			log.Println("Connection closed. Cancelling read operation...")
			return
		}

		dbErr := db.Update(func(txn *badger.Txn) error {
			if !StillHaveConnection(db) {
				return nil
			}
			if _, err := txn.Get([]byte(msg.EventId)); err == badger.ErrKeyNotFound {
				if !StillHaveConnection(db) {
					return nil
				}

				msg.ElapsedTime = fmt.Sprintf("%s", time.Since(msg.ProcessedAt))
				if currentConfig.ReadToFile {
					DumpMessage(msg, filepath.Join(GetDataDumpDirBasedOnTime(msg.ProcessedAt), msg.DumpFilename))
				}

				err = txn.Set([]byte(msg.EventId), msg.Serialize())

				return err
			}
			return nil
		})

		HandleError("Failed to process received Message.", dbErr, true)
	}
}
