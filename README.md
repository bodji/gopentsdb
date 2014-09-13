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
put := gopentsdb.NewPut("disk_occupation", tags, 13.37)

// Send it to OpenTSDB
opentsdb.Put( put )

```

Your are probably wondering why there's no error check on Put. 

The main reason is that the module is launching a goroutine to periodically check the state of the OpenTSDB server by sending "version" to the socket.


### Todo
* Replace forbidden characters in metric name, tags name, and tags values
* Stack puts while opentsdb is not available to send them later
