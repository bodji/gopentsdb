package gopentsdb

import (
	"testing"
	"fmt"
)

func Test_NewPut(t *testing.T) {

	// Tags
	tags := make(map[string]string)
	tags["load"] 		= "load15"
	tags["hostname"]	= "localhost"


	put := NewPut("hardware_load_average", tags, 1.05)
	fmt.Println(put.ToString())

}
