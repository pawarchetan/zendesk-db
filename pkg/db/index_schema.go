package db

import (
	"fmt"
	"github.com/pawarchetan/zendesk-db/pkg/index"
)

// IndexSchema is the schema for an index. An index defines how a table is queried.
// Name of the index. This must be unique among a tables set of indexes.
// This must match the key in the map of Indexes for a TableSchema.
type IndexSchema struct {
	Name    string
	Indexer index.Indexer
}

func (s *IndexSchema) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("missing index name")
	}
	if s.Indexer == nil {
		return fmt.Errorf("missing index function for '%s'", s.Name)
	}
	switch s.Indexer.(type) {
	case index.SingleIndexer:
	case index.MultiIndexer:
	default:
		return fmt.Errorf("index for '%s' must be a SingleIndexer or MultiIndexer", s.Name)
	}
	return nil
}
