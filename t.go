package dbstorage

import (
	"database/sql"
)

type QueryBuilder interface {
	Se(cols string) QueryBuilder
	Fr(tabls string) QueryBuilder
	WR(col string, op string, value string, raw bool, ags ...interface{}) QueryBuilder
	Wr(col string, op string, value string) QueryBuilder
	Wh(col string, value string) QueryBuilder
	Or(col string, order string) QueryBuilder
	Lm(limit int64) QueryBuilder
	Of(offset int64) QueryBuilder
	Up(table string, col string, value string) QueryBuilder
	Ins(table string, values ...interface{}) Executable
	InsI(table string, strct interface{}) Executable
	Del(table string) QueryBuilder
	Executable
}

// Executable is any object who represents a query that can be called on to produce a sql.Rows
type Executable interface {
	Exe() *sql.Rows
}

// Scannable can take in Rows and return an object
type Scannable interface {
	Scan(rows *sql.Rows) Scannable
}
