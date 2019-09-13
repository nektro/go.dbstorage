package dbstorage

import (
	"database/sql"
)

type Database interface {
	Ping() error
	Close() error
	DB() *sql.DB
	CreateTable(name string, pk []string, columns [][]string)
	CreateTableStruct(name string, v interface{})
	DoesTableExist(table string) bool
	Query(modify bool, q string) *sql.Rows
	QueryColumnList(table string) []string
	QueryNextID(table string) int
	QueryPrepared(modify bool, q string, args ...interface{}) *sql.Rows
	QueryDoSelectAll(table string) *sql.Rows
	QueryDoSelect(table string, where string, search string) *sql.Rows
	QueryDoSelectAnd(table string, where string, search string, where2 string, search2 string) *sql.Rows
	QueryDoUpdate(table string, col string, value string, where string, search string)
	QueryDoSelectAllOrder(table string, order string) *sql.Rows
	QuerySelectFunc(table string, sfunc string, col string, haystack string, needle string) *sql.Rows
	QueryDelete(table string, col string, search string)
}

func QueryHasRows(query *sql.Rows) bool {
	b := query.Next()
	query.Close()
	return b
}
