package gopentsdb

import (
	"fmt"
	"strings"
	"time"
)

type Put struct {
	metricName string
	timestamp  int64
	tags       map[string]string
	value      float64
}

func NewPut(metricName string, tags map[string]string, value float64) (p *Put) {

	p = new(Put)
	p.metricName = metricName
	p.timestamp = time.Now().Unix()
	p.tags = tags
	p.value = value

	return
}

func (p *Put) GetTimestamp() int64 {
	return p.timestamp
}
func (p *Put) SetTimestamp(t int64) {
	p.timestamp = t
}


// ToString return the line that should be pushed to OpenTSDB raw socket
// Example :
// 
//      put loadaverage 1.15 load=load15 hostname=localhost
//
func (p *Put) ToString() (s string) {

	s = fmt.Sprintf("put %s %d %.3f ", p.metricName, p.timestamp, p.value)

	for tagName, _ := range p.tags {

		key := strings.ToLower(strings.Replace(tagName, " ", "_", 0))
		value := strings.Replace(p.tags[tagName], " ", "_", 0)

		s += key + "=" + value + " "
	}

	return s
}
