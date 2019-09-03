package dbstorage

import (
	"database/sql"
)

func QueryHasRows(query *sql.Rows) bool {
	b := query.Next()
	query.Close()
	return b
}
