package gopentsdb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

var opentsdbConnection net.Conn
var opentsdbWriteLock *sync.RWMutex

type OpenTsdb struct {
	TsdAddress string
	TsdPort    int

	connected bool
	verbose   bool
}

func NewOpenTsdb(address string, port int, verbose bool) (this *OpenTsdb) {

	this = new(OpenTsdb)
	this.TsdAddress = address
	this.TsdPort = port
	this.connected = false
	this.verbose = verbose

	// Init mutex
	opentsdbWriteLock = new(sync.RWMutex)

	// Launch goroutine to periodically check
	go func() {
		for {
			if !this.StillAlive() {

				// Connection to OpenTSDB
				connection, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", this.TsdAddress, this.TsdPort), time.Second*2)
				if err != nil {
					this.connected = false
					log.Printf("Failed to connect to OpenTSDB : %s\n", err)
				} else {
					this.connected = true
					opentsdbConnection = connection
				}
			}

			time.Sleep(time.Second)
		}
	}()

	return
}

func (this *OpenTsdb) Put(p *Put) (i int, err error) {

	// Are we connected to OpenTSDB ?
	if !this.connected {
		return -1, errors.New("gopentsdb: Can't put, not connected")
	}

	// Lock
	opentsdbWriteLock.Lock()
	defer opentsdbWriteLock.Unlock()

	// Put
	i, err = fmt.Fprintf(opentsdbConnection, p.ToString()+"\n")
	if err != nil {
		return i, err
	}

	// Log?
	if this.verbose {
		log.Printf("[GOPENTSDB] " + p.ToString() + "\n")
	}

	return i, err
}

func (this *OpenTsdb) StillAlive() bool {

	// Test nil
	if opentsdbConnection == nil {
		return false
	}

	// Lock before write/read socket
	opentsdbWriteLock.Lock()
	defer opentsdbWriteLock.Unlock()

	// Send "version" to socket
	fmt.Fprintf(opentsdbConnection, "version"+"\n")

	// Get content
	opentsdbConnection.SetReadDeadline(time.Now().Add(time.Second))
	completeOutput := new(bytes.Buffer)

	for {
		reply := make([]byte, 512)
		read_len, err := opentsdbConnection.Read(reply)
		if err != nil {
			break
		}
		completeOutput.Write(reply[:read_len])
	}

	// Test response
	if strings.Contains(completeOutput.String(), "net.opentsdb") {
		return true
	}

	return false
}
