package db

import (
	"fmt"
	"github.com/pawarchetan/zendesk-search-engine/internal/indexer"
	"github.com/pawarchetan/zendesk-search-engine/internal/tree"
	"sync/atomic"
	"unsafe"
)

const (
	id = "id"
)

// tableIndex is a tuple of (Table, Index) used for lookups
type tableIndex struct {
	Table string
	Index string
}

// Transaction is a transaction against a InMemoryDB.
type Transaction struct {
	db      *InMemoryDB
	rootTxn *tree.Transaction
	content map[tableIndex]*tree.Transaction
}

func (txn *Transaction) read(table, index string) *tree.Transaction {
	if	txn.content != nil {
		key := tableIndex{table, index}
		exist, ok := txn.content[key]
		if ok {
			return exist.Clone()
		}
	}

	path := indexPath(table, index)
	raw, _ := txn.rootTxn.Get(path)
	indexTxn := raw.(*tree.Tree).Transaction()
	return indexTxn
}

func (txn *Transaction) write(table, index string) *tree.Transaction {
	if txn.content == nil {
		txn.content = make(map[tableIndex]*tree.Transaction)
	}

	key := tableIndex{table, index}
	exist, ok := txn.content[key]
	if ok {
		return exist
	}

	path := indexPath(table, index)
	raw, _ := txn.rootTxn.Get(path)
	indexTxn := raw.(*tree.Tree).Transaction()

	txn.content[key] = indexTxn
	return indexTxn
}

// Abort is used to cancel this transaction.
func (txn *Transaction) Abort() {
	if txn.rootTxn == nil {
		return
	}

	txn.rootTxn = nil
	txn.content = nil
}

// Commit is used to finalize this transaction.
func (txn *Transaction) Commit() {
	for key, subTxn := range txn.content {
		path := indexPath(key.Table, key.Index)
		final := subTxn.Commit()
		txn.rootTxn.Insert(path, final)
	}

	newRoot := txn.rootTxn.Commit()
	atomic.StorePointer(&txn.db.root, unsafe.Pointer(newRoot))

	txn.rootTxn = nil
	txn.content = nil
}

// Insert is used to add or update an object into the given table.
func (txn *Transaction) Insert(table string, obj interface{}) error {
	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok {
		return fmt.Errorf("invalid table '%s'", table)
	}

	// Get the primary ID of the object
	idSchema := tableSchema.Indexes[id]
	idIndexer := idSchema.Indexer.(indexer.SingleIndexer)
	ok, idVal, err := idIndexer.FromObject(obj)
	if err != nil {
		return fmt.Errorf("failed to build primary index: %v", err)
	}
	if !ok {
		return fmt.Errorf("object missing primary index")
	}

	for name, indexSchema := range tableSchema.Indexes {
		indexTxn := txn.write(table, name)

		var (
			ok     bool
			values [][]byte
			err    error
		)
		switch indexerType := indexSchema.Indexer.(type) {
		case indexer.SingleIndexer:
			var val []byte
			ok, val, err = indexerType.FromObject(obj)
			values = [][]byte{val}
		case indexer.MultiIndexer:
			ok, values, err = indexerType.FromObject(obj)
		}
		if err != nil {
			return fmt.Errorf("failed to build indexerType '%s': %v", name, err)
		}

		if ok {
			for i := range values {
				values[i] = append(values[i], idVal...)
			}
		}

		for _, val := range values {
			indexTxn.Insert(val, obj)
		}
	}
	return nil
}

// First is used to return the first matching object for the given constraints on the index
func (txn *Transaction) First(table, index string, args ...interface{}) (interface{}, error) {
	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, err
	}

	indexTxn := txn.read(table, indexSchema.Name)

	iter := indexTxn.Root().Iterator()
	iter.SeekPrefix(val)
	_, value, _ := iter.Next()
	return value, nil
}

func (txn *Transaction) getIndexValue(table, index string, args ...interface{}) (*IndexSchema, []byte, error) {
	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok {
		return nil, nil, fmt.Errorf("invalid table '%s'", table)
	}

	indexSchema, ok := tableSchema.Indexes[index]
	if !ok {
		return nil, nil, fmt.Errorf("invalid index '%s'", index)
	}

	if len(args) == 0 {
		return indexSchema, nil, nil
	}

	val, err := indexSchema.Indexer.FromArgs(args...)
	if err != nil {
		return indexSchema, nil, fmt.Errorf("index error: %v", err)
	}
	return indexSchema, val, err
}

// ResultIterator is used to iterate over a list of results from a query on a table.
type ResultIterator interface {
	Next() interface{}
}

// Get is used to construct a ResultIterator over all the rows that match the given constraints of an index.
func (txn *Transaction) Get(table, index string, args ...interface{}) (ResultIterator, error) {
	indexIter, _, err := txn.getIndexIterator(table, index, args...)
	if err != nil {
		return nil, err
	}

	//indexIter.SeekPrefix(val)

	iter := &radixIterator{
		iter:    indexIter,
	}
	return iter, nil
}

func (txn *Transaction) getIndexIterator(table, index string, args ...interface{}) (*tree.Iterator, []byte, error) {
	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, nil, err
	}

	indexTxn := txn.read(table, indexSchema.Name)
	indexRoot := indexTxn.Root()

	indexIter := indexRoot.Iterator()
	return indexIter, val, nil
}

type radixIterator struct {
	iter    *tree.Iterator
}

func (r *radixIterator) Next() interface{} {
	_, value, ok := r.iter.Next()
	if !ok {
		return nil
	}
	return value
}