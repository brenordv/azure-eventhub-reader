set GOOS=windows
set GOARCH=amd64

go build -o hubtools.exe main.go globals.go utils.go db_utils.go eventhub_utils.go file_utils.go parsers.go validators.go wrappers.go
7z a -tzip az-eventhub-reader--windows-amd64--%*.zip *.exe