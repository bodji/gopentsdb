# GOpenTSDB

####Simple golang library to push some metrics to an OpenTSDB instance via raw socket.

### Installation
``
go get github.com/bodji/opentsdb
``
 

### Usage

```Go
opentsdb := gopentsdb.NewOpenTsdb("192.168.1.1", 4242, true, true)

// Create some tags
tags := make(map[string]string)
tags["partition"]   = "1"
tags["disk"]        = "sda"


// Create Put
put := gopentsdb.NewPut("disk_occupation", tags, 13.37)

// Send it to OpenTSDB
_, err := opentsdb.Put( put )
if err != nil {
 log.Printf("Fail to push to OpenTSDB : %s", err)
}

```

### Documentation
http://godoc.org/github.com/bodji/gopentsdb


### Todo
* Replace forbidden characters in metric name, tags name, and tags values
* Stack puts while opentsdb is not available to send them later
