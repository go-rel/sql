# mssql

[![GoDoc](https://godoc.org/github.com/go-rel/mssql?status.svg)](https://pkg.go.dev/github.com/go-rel/mssql)
[![Integration](https://github.com/go-rel/mssql/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/go-rel/mssql/actions/workflows/integration.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-rel/mssql)](https://goreportcard.com/report/github.com/go-rel/mssql)
[![codecov](https://codecov.io/gh/go-rel/mssql/branch/main/graph/badge.svg?token=3VBLHCCG4N)](https://codecov.io/gh/go-rel/mssql)
[![Gitter chat](https://badges.gitter.im/go-rel/rel.png)](https://gitter.im/go-rel/rel)

Microsoft SQL Server adapter for REL.

## Example 

```go
package main

import (
	"context"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-rel/mssql"
	"github.com/go-rel/rel"
)

func main() {
	// open mssql connection.
	adapter, err := mssql.Open("sqlserver://sa:REL2021-mssql@localhost:1433?database=rel")
	if err != nil {
		panic(err)
	}
	defer adapter.Close()

	// initialize REL's repo.
	repo := rel.New(adapter)
	repo.Ping(context.TODO())
}

```

## Supported Driver

- github.com/denisenkom/go-mssqldb

## Supported Database

- Microsoft SQL Server 2017
- Microsoft SQL Server 2019
