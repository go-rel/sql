package builder

import (
	"testing"

	"github.com/go-rel/rel"
	"github.com/stretchr/testify/assert"
)

func TestIndex_Build(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", InlineValues: true, Quoter: &SqlQuoter{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		filter        = Filter{}
		indexBuilder  = Index{
			BufferFactory:    bufferFactory,
			Query:            Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:           filter,
			DropIndexOnTable: true,
			SupportFilter:    true,
		}
	)

	tests := []struct {
		result string
		index  rel.Index
	}{
		{
			result: "CREATE INDEX `index` ON `table` (`column1`);",
			index: rel.Index{
				Op:      rel.SchemaCreate,
				Table:   "table",
				Name:    "index",
				Columns: []string{"column1"},
			},
		},
		{
			result: "CREATE UNIQUE INDEX `index` ON `table` (`column1`);",
			index: rel.Index{
				Op:      rel.SchemaCreate,
				Table:   "table",
				Name:    "index",
				Unique:  true,
				Columns: []string{"column1"},
			},
		},
		{
			result: "CREATE INDEX `index` ON `table` (`column1`, `column2`);",
			index: rel.Index{
				Op:      rel.SchemaCreate,
				Table:   "table",
				Name:    "index",
				Columns: []string{"column1", "column2"},
			},
		},
		{
			result: "CREATE INDEX IF NOT EXISTS `index` ON `table` (`column1`);",
			index: rel.Index{
				Op:       rel.SchemaCreate,
				Table:    "table",
				Name:     "index",
				Optional: true,
				Columns:  []string{"column1"},
			},
		},
		{
			result: "CREATE INDEX IF NOT EXISTS `index` ON `table` (`column1`) COMMENT 'comment';",
			index: rel.Index{
				Op:       rel.SchemaCreate,
				Table:    "table",
				Name:     "index",
				Optional: true,
				Columns:  []string{"column1"},
				Options:  "COMMENT 'comment'",
			},
		},
		{
			result: "CREATE INDEX IF NOT EXISTS `index` ON `table` (`column1`) WHERE `deleted`=false;",
			index: rel.Index{
				Op:       rel.SchemaCreate,
				Table:    "table",
				Name:     "index",
				Optional: true,
				Columns:  []string{"column1"},
				Filter: rel.FilterQuery{
					Type:  rel.FilterEqOp,
					Field: "deleted",
					Value: false,
				},
			},
		},
		{
			result: "CREATE INDEX IF NOT EXISTS `index` ON `table` (`column1`) WHERE `status`='test''s status';",
			index: rel.Index{
				Op:       rel.SchemaCreate,
				Table:    "table",
				Name:     "index",
				Optional: true,
				Columns:  []string{"column1"},
				Filter: rel.FilterQuery{
					Type:  rel.FilterEqOp,
					Field: "status",
					Value: "test's status",
				},
			},
		},
		{
			result: "DROP INDEX `index` ON `table`;",
			index: rel.Index{
				Op:    rel.SchemaDrop,
				Name:  "index",
				Table: "table",
			},
		},
		{
			result: "DROP INDEX IF EXISTS `index` ON `table`;",
			index: rel.Index{
				Op:       rel.SchemaDrop,
				Name:     "index",
				Table:    "table",
				Optional: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			assert.Equal(t, test.result, indexBuilder.Build(test.index))
		})
	}
}
