package node

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/disk/mem"
)

func TestNode(*testing.T) {
	node := New(mem.New())
	putRequest := PutRequest{[]byte("hello"), []byte("world")}
	node.Put(putRequest, &PutResponse{})

	response := GetResponse{}
	getRequest := GetRequest{[]byte("hello")}
	node.Get(getRequest, &response)
	fmt.Println(response)
}
