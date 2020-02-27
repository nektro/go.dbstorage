package dbstorage

import "database/sql"

type Database interface {
	Ping() error
	Close() error
	DB() *sql.DB
	CreateTable(name string, pk []string, columns [][]string)
	CreateTableStruct(name string, v interface{})
	DoesTableExist(table string) bool
	Build() QueryBuilder
	QueryColumnList(table string) []string
	QueryNextID(table string) int64
	QueryPrepared(modify bool, q string, args ...interface{}) *sql.Rows
}

type QueryBuilder interface {
	Se(cols string) QueryBuilder
	Fr(tabls string) QueryBuilder
	WR(col string, op string, value string, raw bool) QueryBuilder
	Wr(col string, op string, value string) QueryBuilder
	Wh(col string, value string) QueryBuilder
	Or(col string, order string) QueryBuilder
	Lm(limit int64) QueryBuilder
	Of(offset int64) QueryBuilder
	Up(table string, col string, value string) QueryBuilder
	Ins(table string, values ...interface{}) Executable
	Executable
}

// Executable is any object who represents a query that can be called on to produce a sql.Rows
type Executable interface {
	Exe() *sql.Rows
}

type Scannable interface {
	Scan(rows *sql.Rows) Scannable
}
