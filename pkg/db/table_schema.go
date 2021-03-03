package db

import (
	"fmt"
	"github.com/pawarchetan/zendesk-db/pkg/index"
)

// TableSchema is the schema for a single table.
// Name: Name of the table. This must match the key in the Tables map in InMemoryDBSchema.
// Indexes: Indexes is the set of indexes for querying this table. The key
// is a unique name for the index and must match the Name in the IndexSchema.
type TableSchema struct {
	Name    string
	Indexes map[string]*IndexSchema
}

// Validate is used to validate the table schema
func (s *TableSchema) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("missing table name")
	}

	if len(s.Indexes) == 0 {
		return fmt.Errorf("missing table indexes for '%s'", s.Name)
	}

	if _, ok := s.Indexes["id"]; !ok {
		return fmt.Errorf("must have id index")
	}

	if _, ok := s.Indexes["id"].Indexer.(index.SingleIndexer); !ok {
		return fmt.Errorf("id index must be a SingleIndexer")
	}

	for name, index := range s.Indexes {
		if name != index.Name {
			return fmt.Errorf("index name mis-match for '%s'", name)
		}

		if err := index.Validate(); err != nil {
			return fmt.Errorf("index %q: %s", name, err)
		}
	}

	return nil
}
