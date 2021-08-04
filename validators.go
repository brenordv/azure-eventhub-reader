package main

import (
	"errors"
	"fmt"
	"path/filepath"
)

// ValidateRunConfiguration loads execution configuration from file.
// Will panic in case of failure.
//
// Parameters:
//  f: filename of the configuration file. used for error messages.
//  op: desired operation. this is extracted from command line.
//
// Returns:
//  Nothing.
func ValidateRunConfiguration(f string, op string) {
	errMsg := fmt.Sprintf("Configuration file '%s' is invalid.", f)

	if currentConfig.Env == "" {
		HandleError(errMsg,
			errors.New("key 'env' is missing or empty"),
			true)
	}

	if currentConfig.EntityPath == "" {
		HandleError(errMsg,
			errors.New("key 'entityPath' is missing or empty"),
			true)
	}

	if currentConfig.ConsumerGroup == "" && op == "read" {
		HandleError(errMsg,
			errors.New("key 'consumerGroup' is missing and I'll not assume $default. really need a consumerGroup to be able to read"),
			true)
	}

	if currentConfig.BadgerValueLogFileSize == 0 {
		currentConfig.BadgerValueLogFileSize = badgerValueLogFileSize
	}

	bDir := GetAppDir()
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

	if currentConfig.OutboundFolder == "" {
		currentConfig.OutboundFolder = filepath.Join(bDir, outboundFolder)
	}

	if currentConfig.OutboundFolderSent == "" {
		currentConfig.OutboundFolderSent = filepath.Join(bDir, outboundFolderSent)
	}

	EnsureDirExists(currentConfig.BadgerBase)
	EnsureDirExists(currentConfig.BadgerDir)
	EnsureDirExists(currentConfig.BadgerValueDir)
	if op == "export2file" || currentConfig.ReadToFile {
		EnsureDirExists(currentConfig.MessageDumpDir)
	}

	if op == "write" {
		EnsureDirExists(currentConfig.OutboundFolder)
		EnsureDirExists(currentConfig.OutboundFolderSent)
	}
}
