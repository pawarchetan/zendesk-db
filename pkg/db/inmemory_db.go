package db

import (
	"github.com/pawarchetan/zendesk-search-engine/internal/tree"
	"sync/atomic"
	"unsafe"
)

// InMemoryDB provides a table abstraction to store objects (rows) with multiple
// indexes based on inserted values. The database makes use of radix
// tree to manage transaction.
type InMemoryDB struct {
	schema  *InMemoryDBSchema
	root    unsafe.Pointer
	primary bool
}

func Init(schema *InMemoryDBSchema) (*InMemoryDB, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	db := &InMemoryDB{
		schema:  schema,
		root:    unsafe.Pointer(tree.New()),
		primary: true,
	}

	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *InMemoryDB) TableSchema() *InMemoryDBSchema {
	return db.schema
}

func (db *InMemoryDB) getRoot() *tree.Tree {
	root := (*tree.Tree)(atomic.LoadPointer(&db.root))
	return root
}

// Transaction is used to start a new transaction in either read or write mode.
func (db *InMemoryDB) Transaction() *Transaction {
	txn := &Transaction{
		db:      db,
		rootTxn: db.getRoot().Transaction(),
	}
	return txn
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a InMemoryDB.
func (db *InMemoryDB) initialize() error {
	root := db.getRoot()
	for tName, tableSchema := range db.schema.Tables {
		for iName := range tableSchema.Indexes {
			index := tree.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.root = unsafe.Pointer(root)
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
