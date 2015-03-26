// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/* Package storage_test provides a means of testing store
functionality which depends on a fully-functional KV client. This
cannot be done within the storage package because of circular
dependencies.

By convention, tests in package storage_test have names of the form
client_*.go.
*/
package storage_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// createTestStore creates a test store using an in-memory
// engine. The caller is responsible for closing the store on exit.
func createTestStore(t *testing.T) *storage.Store {
	return createTestStoreWithEngine(t,
		engine.NewInMem(proto.Attributes{}, 10<<20),
		hlc.NewClock(hlc.NewManualClock(0).UnixNano),
		true)
}

// createTestStoreWithEngine creates a test store using the given engine and clock.
// The caller is responsible for closing the store on exit.
func createTestStoreWithEngine(t *testing.T, eng engine.Engine, clock *hlc.Clock,
	bootstrap bool) *storage.Store {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := gossip.New(rpcContext, gossip.TestInterval, "")
	lSender := kv.NewLocalSender()
	sender := kv.NewTxnCoordSender(lSender, clock, false)
	db := client.NewKV(sender, nil)
	db.User = storage.UserRoot
	// TODO(bdarnell): arrange to have the transport closed.
	store := storage.NewStore(clock, eng, db, g, multiraft.NewLocalRPCTransport(),
		storage.TestStoreConfig)
	if bootstrap {
		if err := store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}); err != nil {
			t.Fatal(err)
		}
	}
	lSender.AddStore(store)
	if bootstrap {
		if err := store.BootstrapRange(); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Start(); err != nil {
		t.Fatal(err)
	}
	return store
}

type multiTestContext struct {
	manualClock *hlc.ManualClock
	clock       *hlc.Clock
	gossip      *gossip.Gossip
	sender      *kv.LocalSender
	transport   multiraft.Transport
	db          *client.KV
	engines     []engine.Engine
	stores      []*storage.Store
}

func (m *multiTestContext) Start(t *testing.T, numStores int) {
	if m.manualClock == nil {
		m.manualClock = hlc.NewManualClock(0)
	}
	if m.clock == nil {
		m.clock = hlc.NewClock(m.manualClock.UnixNano)
	}
	if m.gossip == nil {
		rpcContext := rpc.NewContext(m.clock, rpc.LoadInsecureTLSConfig())
		m.gossip = gossip.New(rpcContext, gossip.TestInterval, "")
	}
	if m.transport == nil {
		m.transport = multiraft.NewLocalRPCTransport()
	}
	if m.sender == nil {
		m.sender = kv.NewLocalSender()
	}
	if m.db == nil {
		txnSender := kv.NewTxnCoordSender(m.sender, m.clock, false)
		m.db = client.NewKV(txnSender, nil)
		m.db.User = storage.UserRoot
	}

	for i := 0; i < numStores; i++ {
		m.addStore(t)
	}
}

func (m *multiTestContext) Stop() {
	for _, eng := range m.engines {
		eng.Stop() // Remove extra engine reference count. See addStore() for details.
	}
	for _, store := range m.stores {
		store.Stop()
	}
	m.transport.Close()
}

// AddStore creates a new store on the same Transport but doesn't create any ranges.
func (m *multiTestContext) addStore(t *testing.T) {
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	store := storage.NewStore(m.clock, eng, m.db, m.gossip, m.transport, storage.TestStoreConfig)
	err := store.Bootstrap(proto.StoreIdent{
		NodeID:  proto.NodeID(len(m.stores) + 1),
		StoreID: proto.StoreID(len(m.stores) + 1),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(m.stores) == 0 {
		// Bootstrap the initial range on the first store
		if err := store.BootstrapRange(); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Start(); err != nil {
		t.Fatal(err)
	}
	// Add an extra reference count to the engine so that the underlying
	// rocksdb instance is not closed when stopping and restarting the
	// engine.
	eng.Start()
	m.engines = append(m.engines, eng)
	m.stores = append(m.stores, store)
	m.sender.AddStore(store)
}

// StopStore stops a store but leaves the engine intact.
// All stopped stores must be restarted before multiTestContext.Stop is called.
func (m *multiTestContext) StopStore(i int) {
	m.stores[i].Stop()
	m.stores[i] = nil
}

// RetartStore restarts a store previously stopped with StopStore.
func (m *multiTestContext) RestartStore(i int, t *testing.T) {
	m.stores[i] = storage.NewStore(m.clock, m.engines[i], m.db, m.gossip, m.transport,
		storage.TestStoreConfig)
	if err := m.stores[i].Start(); err != nil {
		t.Fatal(err)
	}
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, raftID int64, storeID proto.StoreID) (*proto.GetRequest, *proto.GetResponse) {
	args := &proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
	reply := &proto.GetResponse{}
	return args, reply
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, raftID int64, storeID proto.StoreID) (*proto.PutRequest, *proto.PutResponse) {
	args := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Value: proto.Value{
			Bytes: value,
		},
	}
	reply := &proto.PutResponse{}
	return args, reply
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, raftID int64, storeID proto.StoreID) (*proto.IncrementRequest, *proto.IncrementResponse) {
	args := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Increment: inc,
	}
	reply := &proto.IncrementResponse{}
	return args, reply
}

func internalTruncateLogArgs(index uint64, raftID int64, storeID proto.StoreID) (
	*proto.InternalTruncateLogRequest, *proto.InternalTruncateLogResponse) {
	args := &proto.InternalTruncateLogRequest{
		RequestHeader: proto.RequestHeader{
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Index: index,
	}
	reply := &proto.InternalTruncateLogResponse{}
	return args, reply
}

type metaRecord struct {
	key  proto.Key
	desc *proto.RangeDescriptor
}
type metaSlice []metaRecord

// Implementation of sort.Interface.
func (ms metaSlice) Len() int           { return len(ms) }
func (ms metaSlice) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }
func (ms metaSlice) Less(i, j int) bool { return ms[i].key.Less(ms[j].key) }

func meta1Key(key proto.Key) proto.Key {
	return engine.MakeKey(engine.KeyMeta1Prefix, key)
}

func meta2Key(key proto.Key) proto.Key {
	return engine.MakeKey(engine.KeyMeta2Prefix, key)
}

// TestUpdateRangeAddressing verifies range addressing records are
// correctly updated on creation of new range descriptors.
func TestUpdateRangeAddressing(t *testing.T) {
	defer leaktest.AfterTest(t)
	store := createTestStore(t)
	defer store.Stop()

	// When split is false, merging treats the right range as the merged
	// range. With merging, expNewLeft indicates the addressing keys we
	// expect to be removed.
	testCases := []struct {
		split                   bool
		leftStart, leftEnd      proto.Key
		rightStart, rightEnd    proto.Key
		leftExpNew, rightExpNew []proto.Key
	}{
		// Start out with whole range.
		{false, engine.KeyMin, engine.KeyMax, engine.KeyMin, engine.KeyMax,
			[]proto.Key{}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(engine.KeyMax)}},
		// Split KeyMin-KeyMax at key "a".
		{true, engine.KeyMin, proto.Key("a"), proto.Key("a"), engine.KeyMax,
			[]proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}, []proto.Key{meta2Key(engine.KeyMax)}},
		// Split "a"-KeyMax at key "z".
		{true, proto.Key("a"), proto.Key("z"), proto.Key("z"), engine.KeyMax,
			[]proto.Key{meta2Key(proto.Key("z"))}, []proto.Key{meta2Key(engine.KeyMax)}},
		// Split "a"-"z" at key "m".
		{true, proto.Key("a"), proto.Key("m"), proto.Key("m"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("m"))}, []proto.Key{meta2Key(proto.Key("z"))}},
		// Split KeyMin-"a" at meta2(m).
		{true, engine.KeyMin, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("m")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("m"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Split meta2(m)-"a" at meta2(z).
		{true, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("z")), engine.RangeMetaKey(proto.Key("z")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("z"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Split meta2(m)-meta2(z) at meta2(r).
		{true, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("r")), engine.RangeMetaKey(proto.Key("r")), engine.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("r"))}, []proto.Key{meta1Key(proto.Key("z"))}},

		// Now, merge all of our splits backwards...

		// Merge meta2(m)-meta2(z).
		{false, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("r")), engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("r"))}, []proto.Key{meta1Key(proto.Key("z"))}},
		// Merge meta2(m)-"a".
		{false, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("z")), engine.RangeMetaKey(proto.Key("m")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("z"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Merge KeyMin-"a".
		{false, engine.KeyMin, engine.RangeMetaKey(proto.Key("m")), engine.KeyMin, proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("m"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Merge "a"-"z".
		{false, proto.Key("a"), proto.Key("m"), proto.Key("a"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("m"))}, []proto.Key{meta2Key(proto.Key("z"))}},
		// Merge "a"-KeyMax.
		{false, proto.Key("a"), proto.Key("z"), proto.Key("a"), engine.KeyMax,
			[]proto.Key{meta2Key(proto.Key("z"))}, []proto.Key{meta2Key(engine.KeyMax)}},
		// Merge KeyMin-KeyMax.
		{false, engine.KeyMin, proto.Key("a"), engine.KeyMin, engine.KeyMax,
			[]proto.Key{meta2Key(proto.Key("a"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(engine.KeyMax)}},
	}
	expMetas := metaSlice{}

	for i, test := range testCases {
		left := &proto.RangeDescriptor{RaftID: int64(i * 2), StartKey: test.leftStart, EndKey: test.leftEnd}
		right := &proto.RangeDescriptor{RaftID: int64(i*2 + 1), StartKey: test.rightStart, EndKey: test.rightEnd}
		if test.split {
			if err := storage.SplitRangeAddressing(store.DB(), left, right); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := storage.MergeRangeAddressing(store.DB(), left, right); err != nil {
				t.Fatal(err)
			}
		}
		store.DB().Flush()
		// Scan meta keys directly from engine.
		kvs, err := engine.MVCCScan(store.Engine(), engine.KeyMetaPrefix, engine.KeyMetaMax, 0, proto.MaxTimestamp, nil)
		if err != nil {
			t.Fatal(err)
		}
		metas := metaSlice{}
		for _, kv := range kvs {
			scannedDesc := &proto.RangeDescriptor{}
			if err := gogoproto.Unmarshal(kv.Value.Bytes, scannedDesc); err != nil {
				t.Fatal(err)
			}
			metas = append(metas, metaRecord{key: kv.Key, desc: scannedDesc})
		}

		// Continue to build up the expected metas slice, replacing any earlier
		// version of same key.
		addOrRemoveNew := func(keys []proto.Key, desc *proto.RangeDescriptor, add bool) {
			for _, n := range keys {
				found := -1
				for i := range expMetas {
					if expMetas[i].key.Equal(n) {
						found = i
						expMetas[i].desc = desc
						break
					}
				}
				if found == -1 && add {
					expMetas = append(expMetas, metaRecord{key: n, desc: desc})
				} else if found != -1 && !add {
					expMetas = append(expMetas[:found], expMetas[found+1:]...)
				}
			}
		}
		addOrRemoveNew(test.leftExpNew, left, test.split /* on split, add; on merge, remove */)
		addOrRemoveNew(test.rightExpNew, right, true)
		sort.Sort(expMetas)

		if test.split {
			log.V(1).Infof("test case %d: split %q-%q at %q", i, left.StartKey, right.EndKey, left.EndKey)
		} else {
			log.V(1).Infof("test case %d: merge %q-%q + %q-%q", i, left.StartKey, left.EndKey, left.EndKey, right.EndKey)
		}
		for _, meta := range metas {
			log.V(1).Infof("%q", meta.key)
		}
		log.V(1).Infof("")

		if !reflect.DeepEqual(expMetas, metas) {
			t.Errorf("expected metas don't match")
			if len(expMetas) != len(metas) {
				t.Errorf("len(expMetas) != len(metas); %d != %d", len(expMetas), len(metas))
			} else {
				for i, meta := range expMetas {
					if !meta.key.Equal(metas[i].key) {
						fmt.Printf("%d: expected %q vs %q\n", i, meta.key, metas[i].key)
					}
					if !reflect.DeepEqual(meta.desc, metas[i].desc) {
						fmt.Printf("%d: expected %q vs %q and %s vs %s\n", i, meta.key, metas[i].key, meta.desc, metas[i].desc)
					}
				}
			}
		}
	}
}

// TestUpdateRangeAddressingSplitMeta1 verifies that it's an error to
// attempt to update range addressing records that would allow a split
// of meta1 records.
func TestUpdateRangeAddressingSplitMeta1(t *testing.T) {
	defer leaktest.AfterTest(t)
	store := createTestStore(t)
	defer store.Stop()
	left := &proto.RangeDescriptor{StartKey: engine.KeyMin, EndKey: meta1Key(proto.Key("a"))}
	right := &proto.RangeDescriptor{StartKey: meta1Key(proto.Key("a")), EndKey: engine.KeyMax}
	if err := storage.SplitRangeAddressing(store.DB(), left, right); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
