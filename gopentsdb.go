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
	"crypto/tls"
	"strconv"
)

var verbose bool;

type Tsd struct {
	host 	string
	port    int
	ssl		bool

	puts 		chan *Put
	conn 	   	net.Conn
	connected  	bool
	lock	   	sync.Mutex
}

func NewTsd(strconn string, ssl bool, puts chan *Put) (this *Tsd, err error){
	this = new(Tsd)

	items := strings.Split(strconn,":")
	switch len(items){
	case 1:
		this.host = items[0]
		this.port = 4242
	case 2:
		this.host = items[0]
		if this.port, err = strconv.Atoi(items[1]); err != nil{
			return nil, err
		}
	default:
		err = errors.New("gopentsdb: Invalid tsd address " + strconn)
		return nil, err
	}
	this.ssl = ssl

	go func() {
		for {
			if !this.StillAlive() {
				if err := this.Connect() ; err == nil {
					log.Printf("gopentsdb: Connected to %s:%d\n",this.host,this.port)
				} else {
					log.Printf("gopentsdb: Connection failed to %s:%d : %s\n",this.host,this.port,err)
				}
			}
			time.Sleep(time.Duration(10) * time.Second)
		}
	}()

	go func() {
		for {
			if this.connected {
				this.Put(<- puts)
			} else {
				time.Sleep(time.Second)
			}
		}
	}()

	return
}

func (this *Tsd) Connect() (err error) {
	if this.conn, err = net.DialTimeout("tcp", fmt.Sprintf("%s:%d", this.host, this.port), time.Second*2) ; err != nil {
		return
	}
	if this.ssl {
		var tlsconn *tls.Conn
		tlsconn = tls.Client(this.conn,&tls.Config{ InsecureSkipVerify: true })
		if err = tlsconn.Handshake() ; err != nil {
			return
		}
		this.conn = tlsconn
	}
	this.connected = true
	return
}

func (this *Tsd) StillAlive() bool {
	// Test nil
	if this.conn == nil {
		return false
	}

	// Lock before write/read socket
	this.lock.Lock()
	this.lock.Unlock()

	// Send "version" to socket
	fmt.Fprintf(this.conn, "version"+"\n")

	// Get content
	this.conn.SetReadDeadline(time.Now().Add(time.Duration(5) * time.Second))
	reply := new(bytes.Buffer)

	for {
		buf := make([]byte, 512)
		read_len, err := this.conn.Read(buf)
		if err != nil {
			break
		}
		reply.Write(buf[:read_len])
	}

	// Test response
	if strings.Contains(reply.String(), "net.opentsdb") {
		return true
	}

	log.Printf("gopentsdb: Connection lost to %s:%d\n",this.host,this.port)
	this.connected = false
	this.conn.Close()
	return false
}

func (this *Tsd) Put(p *Put) (i int, err error) {
	// Lock
	this.lock.Lock()
	defer this.lock.Unlock()

	// Are we still connected to OpenTSDB ?
	if !this.connected {
		return -1, errors.New("gopentsdb: Can't put, not connected")
	}

	// Put
	i, err = fmt.Fprintf(this.conn, p.ToString()+"\n")
	if err != nil {
		return i, err
	}

	// Log?
	if verbose {
		log.Printf("gopentsdb: " + p.ToString() + "\n")
	}

	return i, err
}

type OpenTsdb struct {
	tsds	  			[]*Tsd
	puts				chan *Put
	deduplication		int
	deduplicationMap  	map[string]*Put
}

// GopenTSDB - Constructor
// Params : address, port, verbose, deduplication
// - verbose is a boolean to enable some logs (be careful, there is a log per put)
// - deduplication is a boolean to enable deduplication of puts in a 10 minutes range
//   (that means that if a put has the same value, tags, and metric, on a 10 minute period, it will be pushed once to OpenTSDB)
// - ssl enable ssl on tsd socket. Certificate is not verified
// TODO handle flush before shutdown
func NewOpenTsdb(hosts []string, ssl bool, deduplication int, buffersize int) (this *OpenTsdb, err error) {
	this = new(OpenTsdb)

	this.puts = make(chan *Put, buffersize)

	this.tsds = make([]*Tsd,len(hosts))
	for i, host := range hosts {
		if this.tsds[i], err = NewTsd(host,ssl,this.puts); err != nil {
			return nil, err
		}
	}

	this.deduplication = deduplication

	// Launch goroutine to clean dedup map
	if deduplication > 0 {
		this.deduplicationMap = make(map[string]*Put)
		go func() {
			for {
				currentTimestamp := time.Now().Unix()
				for putFootPrint := range this.deduplicationMap {
					if currentTimestamp - this.deduplicationMap[putFootPrint].timestamp > int64(deduplication) {
						delete(this.deduplicationMap, putFootPrint)
					}
				}
				time.Sleep(time.Second * 60)
			}
		}()
	}

	return
}

func (this *OpenTsdb) Put(put *Put) (err error){
	// Duplicate ?
	if this.deduplication > 0 && this.IsDuplicate( put ) {
		if verbose {
			log.Printf("gopentsdb: discarding " + put.ToString() + " : duplicate\n")
			return
		}
	}

	select {
	case this.puts <- put:
	default:
		if verbose {
			log.Printf("gopentsdb: discarding " + put.ToString() + " : channel is full\n")
		}
	}

	// TODO figure out a way to detect and propagate failure properly
	return
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

                // Update point (keep timestamp)
                p.timestamp = previousPut.timestamp
			}
		}

        // Update point
        this.deduplicationMap[ putFootPrint ] = p


	} else {
		this.deduplicationMap[ putFootPrint ] = p
	}

	return duplicate
}

func Verbose(enabled bool){
	verbose = enabled
}
