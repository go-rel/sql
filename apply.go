package sql

import (
	"context"

	"github.com/go-rel/rel"
)

// SchemaApply adapter.
type SchemaApply struct {
	exec         Exec
	tableBuilder TableBuilder
	indexBuilder IndexBuilder
}

// SchemaApply performs migration to database.
func (sa SchemaApply) SchemaApply(ctx context.Context, migration rel.Migration) error {
	var (
		statement string
	)

	switch v := migration.(type) {
	case rel.Table:
		statement = sa.tableBuilder.Build(v)
	case rel.Index:
		statement = sa.indexBuilder.Build(v)
	case rel.Raw:
		statement = string(v)
	}

	_, _, err := sa.exec.Exec(ctx, statement, nil)
	return err
}

// Apply performs migration to database.
//
// Deprecated: Use Schema Apply instead.
func (sa SchemaApply) Apply(ctx context.Context, migration rel.Migration) error {
	return sa.SchemaApply(ctx, migration)
}

// NewApplyAdapter adapter.
func NewSchemaApply(exec Exec, tableBuilder TableBuilder, indexBuilder IndexBuilder) SchemaApply {
	return SchemaApply{
		exec:         exec,
		tableBuilder: tableBuilder,
		indexBuilder: indexBuilder,
	}
}
