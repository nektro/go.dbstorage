package dbstorage

import (
	"database/sql"
	"sync"
)

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
	Ins(table string) *sync.Mutex
	Se(cols string) QueryBuilder
	Fr(tabls string) QueryBuilder
	WR(col string, op string, value string, raw bool) QueryBuilder
	Wr(col string, op string, value string) QueryBuilder
	Wh(col string, value string) QueryBuilder
	Or(col string, order string) QueryBuilder
	Up(table string, col string, value string) QueryBuilder
	//
	Exe() *sql.Rows
}

func QueryHasRows(query *sql.Rows) bool {
	b := query.Next()
	query.Close()
	return b
}

type Scannable interface {
	Scan(rows *sql.Rows) Scannable
}

func ScanAll(qb QueryBuilder, s Scannable) []Scannable {
	result := []Scannable{}
	rows := qb.Exe()
	for rows.Next() {
		result = append(result, s.Scan(rows))
	}
	rows.Close()
	return result
}
