// Package disk provides an abstraction over non-replicated (local-node) durable storage.
//
// Lot's of things here are probably not the final API we want
//
// Assumptions / goals:
// - provides single node durability
// - exposes a byte array KV api mostly likely targeting an LSM SST
// - allow embedding multiple disk instances per node (e.g. 1 per physical device)
// - plugable (abstraction over rocksdb but maybe just flat file or other)
// - sepration of the raw KV API from tuning of the backend (e.g. rocksdb specific tuning flags)
package disk

// TODO(shawn) better name matching the go style (e.g. Stringer)?
type Disk interface {
	// We'll want more than just key and val in this api (timestamps, txn semantics)
	Put(key, val []byte)
	Get(key []byte) []byte
}
