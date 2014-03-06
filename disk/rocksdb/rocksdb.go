// Package rocksdb implements the Disk interface backed by a rocks DB instance
package rocksdb

// TODO(shawn) land https://github.com/cockroachdb/gorocks/pull/1
// integrate this backend with our gorocks bridge.

type rocksdbDisk struct {
	// db gorocks.DB
}

// how do we want to allow configure tuning options e.g. compaction at this level.
func New() *Disk {
	return &rocksdbDisk{}
}

func (d *rocksdbDisk) Put(key []byte, val []byte) {
	panic
}

func (d *rocksdbDisk) Get(key []byte) []byte {
	panic
}
