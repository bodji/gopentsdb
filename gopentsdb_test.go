package gopentsdb

import (
	"fmt"
	"testing"
	"time"
)

func Test_NewPut(t *testing.T) {

	// Tags
	tags := make(map[string]string)
	tags["load"] = "load15"
	tags["hostname"] = "localhost"

	put := NewPut("hardware_load_average", tags, 1.05)
	fmt.Println(put.ToString())
}

func Test_Functional(t *testing.T) {
	Verbose(true)
	if tsdb, err := NewOpenTsdb([]string{"127.0.0.1","127.0.0.1"},false,0,10) ; err == nil {
		for i:=0 ; i<1000 ; i++ {
			// Tags
			tags := make(map[string]string)
			tags["load"] = "load15"
			tags["hostname"] = "localhost"

			put := NewPut("hardware_load_average", tags, 1.05)
			fmt.Println(put.ToString())
			tsdb.Put(put)
			time.Sleep(time.Duration(100)*time.Millisecond)
		}
	} else {
		fmt.Println(err)
	}
}
