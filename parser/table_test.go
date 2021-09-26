package parser

import (
	"testing"

	"github.com/go-rel/rel"
	"github.com/stretchr/testify/assert"
)

func TestTable_Parse(t *testing.T) {
	var (
		parser = Table{
			Regexp: TableRegexp("`", "`"),
			ColumnParser: Column{
				Regexp: ColumnRegexp("`", "`"),
			},
		}
		sql = "CREATE TABLE `points` (\n" +
			"	`id` int unsigned NOT NULL AUTO_INCREMENT,\n" +
			"	`created_at` datetime DEFAULT NULL,\n" +
			"	`updated_at` datetime DEFAULT NULL,\n" +
			"	`name` varchar(255) DEFAULT NULL,\n" +
			"	`count` int DEFAULT NULL,\n" +
			"	`score_id` int unsigned DEFAULT NULL,\n" +
			"	PRIMARY KEY (`id`),\n" +
			"	KEY `score_id` (`score_id`),\n" +
			"	CONSTRAINT `points_ibfk_1` FOREIGN KEY (`score_id`) REFERENCES `scores` (`id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
		schema = rel.Schema{}
		table  = rel.Table{Op: rel.SchemaCreate, Name: "points", Options: "ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"}
	)

	println(TableRegexp("`", "`").String())

	table.Int("id", rel.Unsigned(true), rel.Required(true), rel.Options("AUTO_INCREMENT"))
	table.DateTime("created_at")
	table.DateTime("updated_at")
	table.String("name", rel.Limit(255))
	table.Int("count")
	table.Int("score_id", rel.Unsigned(true))
	table.PrimaryKey("id")
	// TODO: KEY `score_id` (`score_id`)
	table.ForeignKey("score_id", "scores", "id", rel.Name("points_ibfk_1"))

	parser.Parse(sql, &schema)
	assert.Equal(t, table, schema.Migrations[0])
}
