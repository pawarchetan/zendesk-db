package tree

import (
	"bytes"
	"sort"
)

// WalkFn is used when walking the tree. Takes a key and value, returning if iteration should be terminated.
type WalkFn func(k []byte, v interface{}) bool

// leafNode is used to represent a value
type leafNode struct {
	mutateCh chan struct{}
	key      []byte
	val      interface{}
}

// edge is used to represent an edge node
type edge struct {
	label byte
	node  *Node
}

// Node is an immutable node in the radix tree
type Node struct {
	leaf *leafNode
	prefix []byte
	edges edges
}

func (n *Node) isLeaf() bool {
	return n.leaf != nil
}

func (n *Node) addEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

func (n *Node) replaceEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *Node) getEdge(label byte) (int, *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node) GetWatch(k []byte) (interface{}, bool) {
	search := k
	for {
		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf.val, true
			}
			break
		}

		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	return nil, false
}

func (n *Node) Get(k []byte) (interface{}, bool) {
	val, ok := n.GetWatch(k)
	return val, ok
}

func (n *Node) Iterator() *Iterator {
	return &Iterator{node: n}
}