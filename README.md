# GOpenTSDB

####Simple golang library to push some metrics to an OpenTSDB instance via raw socket.

### Installation
``
go get github.com/bodji/opentsdb
``
 

### Usage

```Go
opentsdb := gopentsdb.NewOpenTsdb("192.168.1.1", 4242, true)

// Create some tags
tags := make(map[string]string)
tags["partition"]   = "1"
tags["disk"]        = "sda"


// Create Put
put := gopentsdb.Put("disk_occupation", tags, 13.37)

```

Your are probably wondering why there's no error check on Put. 

The main reason is that the module is launching a goroutine to periodically check the state of the OpenTSDB server by sending "version" to the socket.

