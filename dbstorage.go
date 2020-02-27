package dbstorage

import (
	"database/sql"
	"sync"

	"github.com/spf13/pflag"
)

var (
	StatementDebug bool
)

func init() {
	pflag.BoolVar(&StatementDebug, "dbstorage-debug-sql", false, "Enable this flag to print all executed SQL statements.")
}

var (
	InsertsLock = new(sync.Mutex)
)

func QueryHasRows(query *sql.Rows) bool {
	b := query.Next()
	query.Close()
	return b
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

func ScanFirst(qb QueryBuilder, s Scannable) Scannable {
	rows := qb.Exe()
	defer rows.Close()
	if !rows.Next() {
		return nil
	}
	s = s.Scan(rows)
	return s
}
