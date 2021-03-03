package index

// Indexer is an interface used for defining indexes. Indexes are used
// for efficient lookup of objects in a MemDB table. An Indexer must also
// implement one of SingleIndexer or MultiIndexer.
//
// Indexers are primarily responsible for returning the lookup key as
// a byte slice. The byte slice is the key data in the underlying data storage.
type Indexer interface {
	// FromArgs is called to build the exact index key from a list of arguments.
	FromArgs(args ...interface{}) ([]byte, error)
}

// SingleIndexer is an interface used for defining indexes that generate a
// single value per object
type SingleIndexer interface {
	// FromObject extracts the index value from an object. The return values
	// are whether the index value was found, the index value, and any error
	// while extracting the index value, respectively.
	FromObject(raw interface{}) (bool, []byte, error)
}

// MultiIndexer is an interface used for defining indexes that generate
// multiple values per object. Each value is stored as a seperate index
// pointing to the same object.
type MultiIndexer interface {
	// FromObject extracts index values from an object. The return values
	// are the same as a SingleIndexer except there can be multiple index
	// values.
	FromObject(raw interface{}) (bool, [][]byte, error)
}




