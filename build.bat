set GOOS=windows
set GOARCH=amd64
go build -o azreader.exe main.go

7z a -tzip az-eventhub-reader--windows-amd64--%*.zip *.exe
