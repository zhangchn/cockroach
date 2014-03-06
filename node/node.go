// Package node exposes the local node api
package node

import "github.com/cockroachdb/cockroach/disk"

// Basic RPC API. we probably want to move this to protos over persistent sockets

type PutRequest struct {
	key, val []byte
}

type PutResponse struct {
}

type GetRequest struct {
	key []byte
}

type GetResponse struct {
	val []byte
}

type CockroachNodeService struct {
	d disk.Disk
}

func New(d disk.Disk) *CockroachNodeService {
	return &CockroachNodeService{d: d}
}

func (n *CockroachNodeService) Put(request PutRequest, response *PutResponse) {
	n.d.Put(request.key, request.val)
}

func (n *CockroachNodeService) Get(request GetRequest, response *GetResponse) {
	val := n.d.Get(request.key)
	response.val = val
}
