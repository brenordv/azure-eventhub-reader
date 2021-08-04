package main

import "github.com/dgraph-io/badger/v3"

// StillHaveConnection helps to avoid panic errors when handling abrupt runtime interruptions (ctrl+c or stopping ide
// run). This has to be called everytime before a database operation is performed.
// Simply checks if the DB object is not null and if the connection is not closed.
// Will panic in case of failure.
//
// Parameters:
//  db: db object to check
//
// Returns:
//  true if the connection is still valid. false otherwise.
func StillHaveConnection(db *badger.DB) bool {
	return db != nil && !db.IsClosed()
}

// OpenConnection opens a connection with badgerDb.
// Will panic in case of failure.
//
// Parameters:
//  None.
//
// Returns:
//  db object with and open connection.
func OpenConnection() *badger.DB {
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
	HandleError("To open badger database.", err, true)
	return badgerConnection
}

// CloseConnection closes the connection to badgerDb, if it's open.
// Will panic in case of failure.
//
// Parameters:
//  None.
//
// Returns:
//  Nothing.
func CloseConnection() {
	if badgerConnection == nil || badgerConnection.IsClosed() {
		return
	}

	err := badgerConnection.Close()
	HandleError("Failed to close badger connection.", err, true)
}
