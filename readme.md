# Azure Eventhub Reader
This application has a couple of tools to interact with Azure Eventhub.
I use BadgerDb as database to log received messages.

When running this application, it will try to create all folders needed. 
If we don't have enough permissions and the folders do not exist, execution will fail.

My initial aim was to be able to log 1.000.000 messages/day. Considering the benchmark (see bellow), 
I can say this goal was achieved.

Another important note is: This application was not tested for long term usage. 
Although BadgerDb can handle hundreds of terabytes of data, I have not tested it. 

## Operations supported
- ```read```: continuously read from eventhub and log every message to the database (and to file, if configured to do it)
- ```export2file```: reads the database and saves every message to disk. Reading is made in reverse, so last messages will be dumped to disk first. 
- ```write```: for every file in the outbound directory, a message will be sent to eventhub.

## About saving messages to disk.
Inside ```messageDumpDir```, will be created a folder for each day (YYYY-MM-DD). Messages for that day will
be saved inside that folder. The filename is based on the timestamp of when the message was processed + it's id. 
If the file already exist, it will not be overwritten.

## Benchmark
- Reading messages and logging to database: ~300 messages per second (about 25 million messages / day) 
- Reading messages, logging to database and dumping to disk: ~25 messages per second (about 2.1 million messages / day)
- Exporting all logged messages to disk: ~25 messages per second (about 2.1 million messages / day)


## How to use
```shell
hubtools.exe <operation> [-config=<configuration file>]
```

In the commandline, only the operation is required. However, a configuration file is also required.
If the argument is omitted, the application will try to use ```default.conf.json``` as configuration file. 
If no config file is available, the execution will fail.


## Examples
### Read using default config file
```shell
hubtools.exe read
```

### Read using custom config file
```shell
hubtools.exe read -config=c:\\path\\to\\custom.conf.json
```

### Export all messages using default config file
```shell
hubtools.exe export2file
```

### Read using custom config file
```shell
hubtools.exe export2file -config=c:\\path\\to\\custom.conf.json
```


## How to create a configuration file
```json
{
  "consumerGroup": "required (for reading only) string",
  "eventhubConnString": "required string",
  "entityPath": "required string",
  "env": "required string",
  "readToFile": "optional bool (default: false)",  
  "messageDumpDir": "optional string (default: .\\.data-dump\\eventhub)",
  "badgerBase": "optional string (default: .\\.appdata)",
  "badgerDir": "optional string (default: .\\.appdata\\dir)",
  "badgerValueDir": "optional string (default: .\\.appdata\\valueDir)",
  "badgerValueLogFileSize": "optional int64 (default: 1024 * 1024 * 10)",
  "badgerSkipCompactL0OnClose": "optional bool (default: false)",
  "badgerVerbose": "optional bool (default: false)",
  "dumpOnlyMessageData": "optional bool (default: false)",
  "outboundFolder": "optional string (default: .\\.outbound)",
  "outboundFolderSent": "optional string (default: .\\.outbound\\.sent)",
  "dontMoveSentFiles": "optional bool (default: false)"
}
```
### Config file Properties
- **consumerGroup**: Name of the consumer group that will be used. Must inform even if it's the default one.
- **eventhubConnString**: connection string that will be used to connect to Eventhub.
- **entityPath**: The entity path (name of the eventhub) that will be read.
- **readToFile**: If true will log each message to the database and save it to disk.
- **messageDumpDir**: This is the path that will be used to dump each message.
- **badgerBase**: Passed to BadgerDB constructor.
- **badgerDir**: BadgerDB directory.
- **badgerValueDir**: BadgerDB value directory.
- **badgerValueLogFileSize**: Max size of badger Db.
- **badgerSkipCompactL0OnClose**: if true, will skip compacting the database on close (will speed up things, but will use more storage)
- **badgerVerbose**: if true, will let badger print a bunch of logs.
- **dumpOnlyMessageData**: if true, when dumping a message to disk, will only write the message content (and skip writing all the other message details)
- **env**: name that will be appended to the badger directory. This means keeping messages from development, qa, production, etc. separate.
- **outboundFolder**: every file in this folder will be sent to eventhub as a single message.
- **outboundFolderSent**: after sending each message, by default, the associated file will be moved to this directory
- **dontMoveSentFiles**: if true, will not move the file after sending it as message.



Notes:
1. The option ```readToFile``` will greatly slow down reading process. You can always export everything later.
2. All optional paths a relative to where the executable is located.


# TODO
1. Clean this code and do a bunch of refactoring...