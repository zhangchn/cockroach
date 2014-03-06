// Package mem implements the Disk interface in-memory.
// It's obviously not durable.
// We're further limited that we use the built-in map type and must convert the []byte to strings.
package mem

import "github.com/cockroachdb/cockroach/disk"

type memDisk struct {
	// Go doesn't support []byte as either key or value.
	m map[string]string
}

func New() disk.Disk {
	return &memDisk{m: make(map[string]string)}
}

func (d *memDisk) Put(key []byte, val []byte) {
	d.m[string(key)] = string(val)
}

func (d *memDisk) Get(key []byte) []byte {
	return []byte(d.m[string(key)])
}
