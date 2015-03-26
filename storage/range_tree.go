// Copyright 2015 The Cockroach Authors.
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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"bytes"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

type (
	// RangeTree represents holds the metadata about the RangeTree.  There is
	// only one for the whole cluster.
	RangeTree proto.RangeTree
	// RangeTreeNode represents a node in the RangeTree, each range
	// has only a single node.
	RangeTreeNode proto.RangeTreeNode
)

// set saves the RangeTree node to the db.
func (n *RangeTreeNode) set(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) error {
	if err := engine.MVCCPutProto(batch, ms, engine.RangeTreeNodeKey(n.Key), timestamp, nil, (*proto.RangeTreeNode)(n)); err != nil {
		return err
	}
	return nil
}

// set saves the RangeTreeRoot to the db.
func (t *RangeTree) set(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) error {
	if err := engine.MVCCPutProto(batch, ms, engine.KeyRangeTreeRoot, timestamp, nil, (*proto.RangeTree)(t)); err != nil {
		return err
	}
	return nil
}

// SetupRangeTree creates a new RangeTree.  This should only be called as part of
// store.BootstrapRange.
func SetupRangeTree(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp, startKey proto.Key) error {
	tree := (*RangeTree)(&proto.RangeTree{
		RootKey: startKey,
	})
	node := (*RangeTreeNode)(&proto.RangeTreeNode{
		Key:   startKey,
		Black: true,
	})
	if err := node.set(batch, ms, timestamp); err != nil {
		return err
	}
	if err := tree.set(batch, ms, timestamp); err != nil {
		return err
	}
	return nil
}

// GetRangeTree fetches the RangeTree proto.
func GetRangeTree(batch engine.Engine, timestamp proto.Timestamp) (*RangeTree, error) {
	tree := &proto.RangeTree{}
	ok, err := engine.MVCCGetProto(batch, engine.KeyRangeTreeRoot, timestamp, nil, tree)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree:%s", engine.KeyRangeTreeRoot)
	}
	return (*RangeTree)(tree), nil
}

// getRangeTreeNode return the RangeTree node for the given key.
func getRangeTreeNode(batch engine.Engine, timestamp proto.Timestamp, key proto.Key) (*RangeTreeNode, error) {
	node := &proto.RangeTreeNode{}
	ok, err := engine.MVCCGetProto(batch, engine.RangeTreeNodeKey(key), timestamp, nil, node)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree node:%s",
			engine.RangeTreeNodeKey(key))
	}
	return (*RangeTreeNode)(node), nil
}

// getRootNode returns the RangeTree node for the root of the RangeTree.
func (t *RangeTree) getRootNode(batch engine.Engine, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	return getRangeTreeNode(batch, timestamp, t.RootKey)
}

// equal compares twoo range tree nodes.
func (n *RangeTreeNode) equal(other *RangeTreeNode) bool {
	if n.Black != other.Black ||
		!bytes.Equal(*n.LeftKey, *other.LeftKey) ||
		!bytes.Equal(*n.RightKey, *other.RightKey) {
		// *** add parent keys?
		return false
	}
	return true
}

// InsertRange adds a new range to the RangeTree.  This should only be called
// from operations that create new ranges, such as range.splitTrigger.
func (t *RangeTree) InsertRange(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp, key proto.Key) error {
	root, err := t.getRootNode(batch, timestamp)
	if err != nil {
		return err
	}
	updatedRootNode, err := t.insert(batch, ms, timestamp, root, key)
	if err != nil {
		return err
	}
	if !bytes.Equal(t.RootKey, updatedRootNode.Key) {
		t.RootKey = updatedRootNode.Key
		if err = t.set(batch, ms, timestamp); err != nil {
			return err
		}
	}
	return nil
}

// insert performs the insertion of a new range into the RangeTree.
func (t *RangeTree) insert(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp, node *RangeTreeNode, key proto.Key) (*RangeTreeNode, error) {
	/*
		Remove this?
		node, err := node.walkDownRot23(batch, ms, timestamp, node)
		if err != nil {
			return nil, err
		}
	*/
	if key.Less(node.Key) {
		left, err := getRangeTreeNode(batch, timestamp, *node.LeftKey)
		if err != nil {
			return nil, err
		}
		updatedLeft, err := t.insert(batch, ms, timestamp, left, key)
		if err != nil {
			return nil, err
		}
		if !updatedLeft.equal(left) {
			if err = updatedLeft.set(batch, ms, timestamp); err != nil {
				return nil, err
			}
			// do we need to update node as well?  *****
		}
	} else {
		right, err := getRangeTreeNode(batch, timestamp, *node.RightKey)
		if err != nil {
			return nil, err
		}
		updatedRight, err := t.insert(batch, ms, timestamp, right, key)
		if err != nil {
			return nil, err
		}
		if !updatedRight.equal(right) {
			if err = updatedRight.set(batch, ms, timestamp); err != nil {
				return nil, err
			}
			// do we need to update node as well?  *****
		}
	}

	return node.walkUpRot23(batch, ms, timestamp)
}

func (n *RangeTreeNode) isRed() bool {
	if n == nil {
		return false
	}
	return !n.Black
}

func (n *RangeTreeNode) walkUpRot23(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	// Should we rotate left?
	right, err := getRangeTreeNode(batch, timestamp, *n.RightKey)
	if err != nil {
		return nil, err
	}
	left, err := getRangeTreeNode(batch, timestamp, *n.LeftKey)
	if err != nil {
		return nil, err
	}
	if right.isRed() && !left.isRed() {
		n, err = n.rotateLeft(batch, ms, timestamp)
		if err != nil {
			return nil, err
		}
	}

	// Should we rotate right?
	left, err = getRangeTreeNode(batch, timestamp, *n.LeftKey)
	if err != nil {
		return nil, err
	}
	leftLeft, err := getRangeTreeNode(batch, timestamp, *left.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.isRed() && leftLeft.isRed() {
		n, err = n.rotateRight(batch, ms, timestamp)
		if err != nil {
			return nil, err
		}
	}

	// Should we flip?
	right, err = getRangeTreeNode(batch, timestamp, *n.RightKey)
	if err != nil {
		return nil, err
	}
	left, err = getRangeTreeNode(batch, timestamp, *n.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.isRed() && right.isRed() {
		n, err = n.flip(batch, ms, timestamp)
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

func (n *RangeTreeNode) rotateLeft(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	// **** HERE
	return n, nil
}

func (n *RangeTreeNode) rotateRight(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	// **** HERE
	return n, nil
}

func (n *RangeTreeNode) flip(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	// **** HERE
	return n, nil
}
