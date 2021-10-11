package builder

import (
	"testing"
	"time"

	"github.com/go-rel/rel"
	"github.com/go-rel/sql"
	"github.com/stretchr/testify/assert"
)

func TestTable_Build(t *testing.T) {
	var (
		tableBuilder = Table{
			BufferFactory: BufferFactory{InlineValues: true, BoolTrueValue: "true", BoolFalseValue: "false", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			ColumnMapper:  sql.ColumnMapper,
		}
	)

	tests := []struct {
		result string
		table  rel.Table
	}{
		{
			result: "CREATE TABLE `products` (`id` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(255), `description` TEXT);",
			table: rel.Table{
				Op:   rel.SchemaCreate,
				Name: "products",
				Definitions: []rel.TableDefinition{
					rel.Column{Name: "id", Type: rel.ID},
					rel.Column{Name: "name", Type: rel.String},
					rel.Column{Name: "description", Type: rel.Text},
				},
			},
		},
		{
			result: "CREATE TABLE `columns` (`bool` BOOL NOT NULL DEFAULT false, `int` INT(11) UNSIGNED, `bigint` BIGINT(20) UNSIGNED, `float` FLOAT(24) UNSIGNED, `decimal` DECIMAL(6,2) UNSIGNED, `string` VARCHAR(144) UNIQUE, `text` TEXT(1000), `date` DATE, `datetime` DATETIME DEFAULT '2020-01-01 01:00:00', `time` TIME, `blob` blob, PRIMARY KEY (`int`), FOREIGN KEY (`int`, `string`) REFERENCES `products` (`id`, `name`) ON DELETE CASCADE ON UPDATE CASCADE, UNIQUE `date_unique` (`date`)) Engine=InnoDB;",
			table: rel.Table{
				Op:   rel.SchemaCreate,
				Name: "columns",
				Definitions: []rel.TableDefinition{
					rel.Column{Name: "bool", Type: rel.Bool, Required: true, Default: false},
					rel.Column{Name: "int", Type: rel.Int, Limit: 11, Unsigned: true},
					rel.Column{Name: "bigint", Type: rel.BigInt, Limit: 20, Unsigned: true},
					rel.Column{Name: "float", Type: rel.Float, Precision: 24, Unsigned: true},
					rel.Column{Name: "decimal", Type: rel.Decimal, Precision: 6, Scale: 2, Unsigned: true},
					rel.Column{Name: "string", Type: rel.String, Limit: 144, Unique: true},
					rel.Column{Name: "text", Type: rel.Text, Limit: 1000},
					rel.Column{Name: "date", Type: rel.Date},
					rel.Column{Name: "datetime", Type: rel.DateTime, Default: time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC)},
					rel.Column{Name: "time", Type: rel.Time},
					rel.Column{Name: "blob", Type: "blob"},
					rel.Key{Columns: []string{"int"}, Type: rel.PrimaryKey},
					rel.Key{Columns: []string{"int", "string"}, Type: rel.ForeignKey, Reference: rel.ForeignKeyReference{Table: "products", Columns: []string{"id", "name"}, OnDelete: "CASCADE", OnUpdate: "CASCADE"}},
					rel.Key{Columns: []string{"date"}, Name: "date_unique", Type: rel.UniqueKey},
				},
				Options: "Engine=InnoDB",
			},
		},
		{
			result: "CREATE TABLE IF NOT EXISTS `products` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, `raw` BOOL);",
			table: rel.Table{
				Op:       rel.SchemaCreate,
				Name:     "products",
				Optional: true,
				Definitions: []rel.TableDefinition{
					rel.Column{Name: "id", Type: rel.BigID},
					rel.Raw("`raw` BOOL"),
				},
			},
		},
		{
			result: "ALTER TABLE `columns` ADD COLUMN `verified` BOOL;ALTER TABLE `columns` RENAME COLUMN `string` TO `name`;ALTER TABLE `columns` ;ALTER TABLE `columns` DROP COLUMN `blob`;",
			table: rel.Table{
				Op:   rel.SchemaAlter,
				Name: "columns",
				Definitions: []rel.TableDefinition{
					rel.Column{Name: "verified", Type: rel.Bool, Op: rel.SchemaCreate},
					rel.Column{Name: "string", Rename: "name", Op: rel.SchemaRename},
					rel.Column{Name: "bool", Type: rel.Int, Op: rel.SchemaAlter},
					rel.Column{Name: "blob", Op: rel.SchemaDrop},
				},
			},
		},
		{
			result: "ALTER TABLE `transactions` ADD FOREIGN KEY (`user_id`) REFERENCES `products` (`id`, `name`) ON DELETE CASCADE ON UPDATE CASCADE;",
			table: rel.Table{
				Op:   rel.SchemaAlter,
				Name: "transactions",
				Definitions: []rel.TableDefinition{
					rel.Key{Columns: []string{"user_id"}, Type: rel.ForeignKey, Reference: rel.ForeignKeyReference{Table: "products", Columns: []string{"id", "name"}, OnDelete: "CASCADE", OnUpdate: "CASCADE"}},
				},
			},
		},
		{
			result: "ALTER TABLE `table` RENAME TO `table1`;",
			table: rel.Table{
				Op:     rel.SchemaRename,
				Name:   "table",
				Rename: "table1",
			},
		},
		{
			result: "DROP TABLE `table`;",
			table: rel.Table{
				Op:   rel.SchemaDrop,
				Name: "table",
			},
		},
		{
			result: "DROP TABLE IF EXISTS `table`;",
			table: rel.Table{
				Op:       rel.SchemaDrop,
				Name:     "table",
				Optional: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			assert.Equal(t, test.result, tableBuilder.Build(test.table))
		})
	}
}
