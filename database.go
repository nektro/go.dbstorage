package dbstorage

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/nektro/go-util/util"

	. "github.com/nektro/go-util/alias"
)

// Database represents an active db connection
type Database interface {
	Inner
}

type Inner interface {
	Ping() error
	Close() error
	DB() *sql.DB
	CreateTable(name string, pk []string, columns [][]string)
	DoesTableExist(table string) bool
	Build() QueryBuilder
	QueryColumnList(table string) []string
	QueryNextID(table string) int64
	QueryRowCount(table string) int64
	DropTable(name string)
	DriverName() string
	TagName() string
}
