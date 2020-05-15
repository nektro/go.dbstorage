package dbstorage

import (
	"database/sql"
	"sync"

	"github.com/nektro/go-util/vflag"
)

var (
	// StatementDebug - Enable this flag to print all executed SQL statements.
	StatementDebug bool
)

func init() {
	vflag.BoolVar(&StatementDebug, "dbstorage-debug-sql", false, "Enable this flag to print all executed SQL statements.")
}

var (
	// InsertsLock - use this so that Database.QueryNextID and DataBase.Build.Ins happen in an atomic fashion.
	InsertsLock = new(sync.Mutex)
)

// QueryHasRows checks if a Rows response contains any values, and then closes the query.
func QueryHasRows(query *sql.Rows) bool {
	b := query.Next()
	query.Close()
	return b
}

// ScanAll scans all possible values of a QueryBuilder into an array based on template Scannable.
func ScanAll(qb QueryBuilder, s Scannable) []Scannable {
	result := []Scannable{}
	rows := qb.Exe()
	for rows.Next() {
		result = append(result, s.Scan(rows))
	}
	rows.Close()
	return result
}

// ScanFirst scans the first value from the QueryBuilder, then closes the query.
func ScanFirst(qb QueryBuilder, s Scannable) Scannable {
	rows := qb.Exe()
	defer rows.Close()
	if !rows.Next() {
		return nil
	}
	s = s.Scan(rows)
	return s
}
