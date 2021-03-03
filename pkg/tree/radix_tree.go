package tree

// Tree implements an radix tree. This can be treated as a Dictionary abstract data type.
// The main advantage over a standard hash map is ordered iteration.
type Tree struct {
	root *Node
	size int
}

func New() *Tree {
	t := &Tree{
		root: &Node{},
	}
	return t
}

type Transaction struct {
	root *Node
	size int
}

func (t *Tree) Transaction() *Transaction {
	txn := &Transaction{
		root: t.root,
		size: t.size,
	}
	return txn
}

func (t *Transaction) Clone() *Transaction {
	txn := &Transaction{
		root: t.root,
		size: t.size,
	}
	return txn
}

func (t *Transaction) writeNode(n *Node) *Node {
	nc := &Node{
		leaf:     n.leaf,
	}
	if n.prefix != nil {
		nc.prefix = make([]byte, len(n.prefix))
		copy(nc.prefix, n.prefix)
	}
	if len(n.edges) != 0 {
		nc.edges = make([]edge, len(n.edges))
		copy(nc.edges, n.edges)
	}

	return nc
}

func (t *Transaction) insert(n *Node, k, search []byte, v interface{}) (*Node, interface{}, bool) {
	if len(search) == 0 {
		var oldVal interface{}
		didUpdate := false
		if n.isLeaf() {
			oldVal = n.leaf.val
			didUpdate = true
		}

		nc := t.writeNode(n)
		nc.leaf = &leafNode{
			mutateCh: make(chan struct{}),
			key:      k,
			val:      v,
		}
		return nc, oldVal, didUpdate
	}

	idx, child := n.getEdge(search[0])

	if child == nil {
		e := edge{
			label: search[0],
			node: &Node{
				leaf: &leafNode{
					key:      k,
					val:      v,
				},
				prefix: search,
			},
		}
		nc := t.writeNode(n)
		nc.addEdge(e)
		return nc, nil, false
	}

	commonPrefix := longestPrefix(search, child.prefix)
	if commonPrefix == len(child.prefix) {
		search = search[commonPrefix:]
		newChild, oldVal, didUpdate := t.insert(child, k, search, v)
		if newChild != nil {
			nc := t.writeNode(n)
			nc.edges[idx].node = newChild
			return nc, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	nc := t.writeNode(n)
	splitNode := &Node{
		prefix:   search[:commonPrefix],
	}
	nc.replaceEdge(edge{
		label: search[0],
		node:  splitNode,
	})

	modChild := t.writeNode(child)
	splitNode.addEdge(edge{
		label: modChild.prefix[commonPrefix],
		node:  modChild,
	})
	modChild.prefix = modChild.prefix[commonPrefix:]

	leaf := &leafNode{
		mutateCh: make(chan struct{}),
		key:      k,
		val:      v,
	}

	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leaf = leaf
		return nc, nil, false
	}

	splitNode.addEdge(edge{
		label: search[0],
		node: &Node{
			leaf:     leaf,
			prefix:   search,
		},
	})
	return nc, nil, false
}

func (t *Transaction) Insert(k []byte, v interface{}) (interface{}, bool) {
	newRoot, oldVal, didUpdate := t.insert(t.root, k, k, v)
	if newRoot != nil {
		t.root = newRoot
	}
	if !didUpdate {
		t.size++
	}
	return oldVal, didUpdate
}

// Root returns the current root of the radix tree within this transaction.
func (t *Transaction) Root() *Node {
	return t.root
}

// Get is used to lookup a specific key
func (t *Transaction) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k)
}

// Commit is used to finalize the transaction and return a new tree.
func (t *Transaction) Commit() *Tree {
	nt := &Tree{t.root, t.size}
	return nt
}

// Insert is used to add or update a given key.
func (t *Tree) Insert(k []byte, v interface{}) (*Tree, interface{}, bool) {
	txn := t.Transaction()
	old, ok := txn.Insert(k, v)
	return txn.Commit(), old, ok
}

// Root returns the root node of the tree which can be used for richer query operations.
func (t *Tree) Root() *Node {
	return t.root
}

// Get is used to lookup a specific key, returning the value and if it was found
func (t *Tree) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k)
}

func longestPrefix(k1, k2 []byte) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}