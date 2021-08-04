package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// DumpMessage creates a text file with the Message received.
// Will panic in case of failure.
//
// Parameters:
//  checkpoint: Message with data extracted from the eventhub event.
//  path: path where this checkpoint will be saved.
//
// Returns:
//  Nothing.
func DumpMessage(checkpoint Message, path string) {
	file, err := os.Create(path)
	if err != nil {
		HandleError(fmt.Sprintf("Failed to create file '%s'.", path), err, true)
	} else {
		defer func() {
			if err := file.Close(); err != nil {
				HandleError(fmt.Sprintf("Failed to close file '%s'.", path), err, true)
			}
		}()
		var content string
		if currentConfig.DumpOnlyMessageData {
			content = checkpoint.MsgData
		} else {
			content = checkpoint.ToString()
		}

		_, err = io.WriteString(file, content)
		if err != nil {
			HandleError(fmt.Sprintf("Failed to write to file '%s'.", path), err, true)
		} else {
			defer func() {
				if err := file.Sync(); err != nil {
					HandleError(fmt.Sprintf("Failed to flush file '%s' to disk.", path), err, true)
				}
			}()
		}
	}
}

// EnsureDirExists if the path does not exist, tries to create it.
// Will panic in case of failure.
//
// Parameters:
//  path: path that will be checked.
//
// Returns:
//  Nothing.
func EnsureDirExists(path string) {
	if FileOrDirExists(path) {
		return
	}
	err := os.MkdirAll(path, os.ModePerm)
	HandleError(fmt.Sprintf("Failed to create path '%s'.", path), err, true)
}

// FileOrDirExists returns whether the given file or directory exists.
// Will panic in case of failure.
//
// Parameters:
//  path: path that will be checked.
//
// Returns:
//  true if the path exists.
func FileOrDirExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}

	return !os.IsNotExist(err)
}

// MoveFile will try to move the old file to the new place.
// The argument 'new' must be the full path, including the new filename.
// Will panic if it cannot move file
//
// Parameters:
//  old: source file that will be moved
//  new: new path (including filename) that will be moved
//
// Returns:
//  Nothing.
func MoveFile(old string, new string) {
	err := os.Rename(old, new)
	HandleError(fmt.Sprintf("Failed to move file '%s' to '%s'", old, new), err, true)
}

// ReadTextFile will read a text file and return it's content. If something goes wrong, will explode.
// Will panic read fails.
//
// Parameters:
//  f: path to the file that will be read.
//
// Returns:
//  String content of the file.
func ReadTextFile(f string) string {
	content, err := ioutil.ReadFile(f)
	HandleError(fmt.Sprintf("Failed to read file '%s'", f), err, true)
	return string(content)
}

// ListFiles will return two things: the list of the files found in the directory and the number of files found.
// this is not recursive.
// Will panic if it cannot read directory.
//
// Parameters:
//  dir: path to the directory that will be read
//
// Returns:
//  (list of files, number of files found)
func ListFiles(dir string) ([]string, int) {
	files, err := ioutil.ReadDir(dir)
	var filenames []string
	HandleError(fmt.Sprintf("Failed to list files in directory '%s'.", dir), err, true)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())
		fstat, err := os.Stat(fName)

		if err != nil || fstat.IsDir() {
			continue
		}
		filenames = append(filenames, fName)
	}

	return filenames, len(files)
}
