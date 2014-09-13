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

// Constructor
func NewPut(metricName string, tags map[string]string, value float64) (p *Put) {

	p = new(Put)
	p.metricName = metricName
	p.timestamp = time.Now().Unix()
	p.tags = tags
	p.value = value

	return
}

// Getters / Setters
func (p *Put) GetTimestamp() int64 {
	return p.timestamp
}
func (p *Put) SetTimestamp(t int64) {
	p.timestamp = t
}

// Misc
func (p *Put) ToString() (s string) {

	// Begin of string
	s = fmt.Sprintf("put %s %d %.3f ", p.metricName, p.timestamp, p.value)

	// Tags
	for tagName, _ := range p.tags {

		// Replaces spaces in keys and values
		key := strings.ToLower(strings.Replace(tagName, " ", "_", 0))
		value := strings.ToLower(strings.Replace(p.tags[tagName], " ", "_", 0))

		// Concat
		s += key + "=" + value + " "
	}

	return s
}
