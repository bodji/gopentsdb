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
	"sort"
)

var opentsdbConnection net.Conn
var opentsdbWriteLock *sync.RWMutex

type OpenTsdb struct {
	TsdAddress string
	TsdPort    int

	connected bool
	verbose   bool

	deduplication		bool
	deduplicationMap  	map[string]*Put
}

func NewOpenTsdb(address string, port int, verbose bool, deduplication bool) (this *OpenTsdb) {

	this = new(OpenTsdb)
	this.TsdAddress = address
	this.TsdPort = port
	this.connected = false
	this.verbose = verbose

	this.deduplication = deduplication
	this.deduplicationMap = make(map[string]*Put)

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

	// Launch goroutine to clean dedup map
	if deduplication {
		go func() {
			for {
				currentTimestamp := time.Now().Unix()
				for putFootPrint := range this.deduplicationMap {
					if currentTimestamp - this.deduplicationMap[putFootPrint].timestamp > 600 {
						delete(this.deduplicationMap, putFootPrint)
					}
				}
				time.Sleep(time.Second * 60)
			}
		}()
	}

	return
}

func (this *OpenTsdb) Put(p *Put) (i int, err error) {

	// Are we connected to OpenTSDB ?
	if !this.connected {
		return -1, errors.New("gopentsdb: Can't put, not connected")
	}

	// Duplicate ?
	if this.deduplication && this.IsDuplicate( p ) {
		return 0,nil
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

func (this *OpenTsdb) IsDuplicate(p *Put) ( duplicate bool ) {

	duplicate = false

	// Sort tags
	var sortedTags []string
	for tagName := range p.tags {
		sortedTags = append(sortedTags,tagName)
	}
	sort.Strings(sortedTags)

	// Make put footprint
	putFootPrint := p.metricName
	for i := range sortedTags {
		putFootPrint += sortedTags[i] + p.tags[sortedTags[i]]
	}

	// Check if metric was already pushed before
	if _, ok := this.deduplicationMap[ putFootPrint ]; ok{

		// It exist before, check dedup
		previousPut := this.deduplicationMap[ putFootPrint ]

		if p.timestamp - previousPut.timestamp < 600 {
			if previousPut.value == p.value {
				duplicate = true
			}
		}

	} else {
		this.deduplicationMap[ putFootPrint ] = p
	}

	return duplicate
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
