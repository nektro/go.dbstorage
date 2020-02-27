package dbstorage

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/nektro/go-util/util"

	. "github.com/nektro/go-util/alias"

	_ "github.com/mattn/go-sqlite3"
)

type DbProxy struct {
	db *sql.DB
}

type PragmaTableInfo struct {
	CID        int
	Name       string
	Type       string
	NotNull    bool
	HasDefault bool
	HasPK      bool
}

func ConnectSqlite(path string) Database {
	op := url.Values{}
	op.Add("mode", "rwc")
	op.Add("cache", "shared")
	op.Add("_busy_timeout", "5000")
	op.Add("_journal_mode", "OFF")
	db, err := sql.Open("sqlite3", "file:"+path+"?"+op.Encode())
	util.CheckErr(err)
	db.SetMaxOpenConns(1)
	util.DieOnError(db.Ping())
	return &DbProxy{db}
}

func (db *DbProxy) Ping() error {
	return db.db.Ping()
}

func (db *DbProxy) Close() error {
	return db.db.Close()
}

func (db *DbProxy) DB() *sql.DB {
	return db.db
}

func (db *DbProxy) CreateTable(name string, pk []string, columns [][]string) {
	if !db.DoesTableExist(name) {
		db.Query(true, F("create table %s(%s %s)", name, pk[0], pk[1]))
		util.Log(F("Created table '%s'", name))
	}
	pti := db.QueryColumnList(name)
	for _, col := range columns {
		if !util.Contains(pti, col[0]) {
			db.Query(true, F("alter table %s add %s %s", name, col[0], col[1]))
			util.Log(F("Added column '%s.%s'", name, col[0]))
		}
	}
}

func (db *DbProxy) CreateTableStruct(name string, v interface{}) {
	t := reflect.TypeOf(v)
	cols := [][]string{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		g := f.Tag.Get("sqlite")
		if len(g) > 0 {
			cols = append(cols, []string{f.Tag.Get("json"), g})
		}
	}
	db.CreateTable(name, []string{"id", "bigint primary key"}, cols)
}

func (db *DbProxy) DoesTableExist(table string) bool {
	q := db.Query(false, F("select name from sqlite_master where type='table' AND name='%s';", table))
	e := q.Next()
	q.Close()
	return e
}

func (db *DbProxy) Query(modify bool, q string) *sql.Rows {
	if modify {
		_, err := db.db.Exec(q)
		util.CheckErr(err)
		return nil
	}
	rows, err := db.db.Query(q)
	util.CheckErr(err)
	return rows
}

func (db *DbProxy) QueryTableInfo(table string) []PragmaTableInfo {
	var result []PragmaTableInfo
	rows := db.Query(false, F("pragma table_info(%s)", table))
	for rows.Next() {
		var v PragmaTableInfo
		rows.Scan(&v.CID, &v.Name, &v.Type, &v.NotNull, &v.HasDefault, &v.HasPK)
		result = append(result, v)
	}
	rows.Close()
	return result
}

func (db *DbProxy) QueryColumnList(table string) []string {
	var result []string
	for _, item := range db.QueryTableInfo(table) {
		result = append(result, item.Name)
	}
	return result
}

func (db *DbProxy) QueryNextID(table string) int64 {
	result := int64(0)
	rows := db.Query(false, F("select id from %s order by id desc limit 1", table))
	for rows.Next() {
		rows.Scan(&result)
	}
	rows.Close()
	return result + 1
}

func (db *DbProxy) QueryPrepared(modify bool, q string, args ...interface{}) *sql.Rows {
	stmt, err := db.db.Prepare(q)
	util.CheckErr(err)
	if modify {
		_, err := stmt.Exec(args...)
		util.CheckErr(err)
		return nil
	}
	rows, err := stmt.Query(args...)
	util.CheckErr(err)
	return rows
}

func (db *DbProxy) QueryDoSelectAll(table string) *sql.Rows {
	return db.Query(false, F("select * from %s", table))
}

func (db *DbProxy) QueryDoSelect(table string, where string, search string) *sql.Rows {
	return db.QueryPrepared(false, F("select * from %s where %s = ?", table, where), search)
}

func (db *DbProxy) QueryDoSelectAnd(table string, where string, search string, where2 string, search2 string) *sql.Rows {
	return db.QueryPrepared(false, F("select * from %s where %s = ? and %s = ?", table, where, where2), search, search2)
}

func (db *DbProxy) QueryDoUpdate(table string, col string, value string, where string, search string) {
	db.QueryPrepared(true, F("update %s set %s = ? where %s = ?", table, col, where), value, search)
}

func (db *DbProxy) QueryDoSelectAllOrder(table string, order string) *sql.Rows {
	return db.Query(false, F("select * from %s order by %s desc", table, order))
}

func (db *DbProxy) QuerySelectFunc(table string, sfunc string, col string, haystack string, needle string) *sql.Rows {
	return db.QueryPrepared(false, F("select %s(%s) from %s where %s = ?", sfunc, col, table, haystack), needle)
}

func (db *DbProxy) QueryDelete(table string, col string, search string) {
	db.QueryPrepared(true, F("delete from %s where %s = ?", table, col), search)
}

//

//

type sQueryBuilder struct {
	d *DbProxy    // db
	q string      // query string
	v []string    // values
	m bool        // modify
	w [][4]string // where's
	o [][2]string // order's
	l int64       // limit
	f int64       // offset
}

func (db *DbProxy) Build() QueryBuilder {
	qb := new(sQueryBuilder)
	qb.d = db
	return qb
}

func (qb *sQueryBuilder) Se(cols string) QueryBuilder {
	qb.m = false
	qb.q = qb.q + "select " + cols
	return qb
}

func (qb *sQueryBuilder) Fr(table string) QueryBuilder {
	qb.q = qb.q + " from " + table
	return qb
}

func (qb *sQueryBuilder) WR(col string, op string, value string, raw bool) QueryBuilder {
	qb.w = append(qb.w, [4]string{col, op, value, strconv.FormatBool(raw)})
	return qb
}

func (qb *sQueryBuilder) Wr(col string, op string, value string) QueryBuilder {
	qb.WR(col, op, value, false)
	return qb
}

func (qb *sQueryBuilder) Wh(col string, value string) QueryBuilder {
	qb.Wr(col, "=", value)
	return qb
}

func (qb *sQueryBuilder) Or(col string, order string) QueryBuilder {
	qb.o = append(qb.o, [2]string{col, order})
	return qb
}

func (qb *sQueryBuilder) Lm(limit int64) QueryBuilder {
	qb.l = limit
	return qb
}

func (qb *sQueryBuilder) Of(offset int64) QueryBuilder {
	qb.f = offset
	return qb
}

func (qb *sQueryBuilder) Exe() *sql.Rows {
	vals := []string{}
	vals = append(vals, qb.v...)
	for i, item := range qb.w {
		if item[3] == "false" {
			if i == 0 {
				qb.q += " where " + item[0] + " " + item[1] + " ?"
			} else {
				qb.q += " and " + item[0] + " " + item[1] + " ?"
			}
			vals = append(vals, item[2])
		} else {
			if i == 0 {
				qb.q += " where " + item[0] + " " + item[1] + " " + item[2]
			} else {
				qb.q += " and " + item[0] + " " + item[1] + " " + item[2]
			}
		}
	}
	for i, item := range qb.o {
		if i == 0 {
			qb.q += " order by " + item[0] + " " + item[1]
		} else {
			qb.q += ", " + item[0] + " " + item[1]
		}
	}
	if qb.l > 0 {
		qb.q += " limit " + strconv.FormatInt(qb.l, 10)
	}
	if qb.f > 0 {
		qb.q += " offset " + strconv.FormatInt(qb.f, 10)
	}
	iva := make([]interface{}, len(vals))
	for i, v := range vals {
		iva[i] = v
	}
	if StatementDebug {
		st := bytes.Split(debug.Stack(), []byte("\n"))
		for _, item := range st {
			if item[0] != '\t' {
				continue
			}
			if bytes.Contains(item, []byte("src/runtime/debug")) || bytes.Contains(item, []byte("github.com/nektro/go.dbstorage")) {
				continue
			}
			fmt.Println("---", string(item[1:]), "\t", "-", qb.q)
			break
		}
	}
	return qb.d.QueryPrepared(qb.m, qb.q, iva...)
}

func (qb *sQueryBuilder) Up(table string, col string, value string) QueryBuilder {
	qb.m = true
	qb.q = qb.q + "update " + table + " set " + col + " = ?"
	qb.v = append(qb.v, value)
	return qb
}

func (qb *sQueryBuilder) Ins(table string, values ...interface{}) QueryBuilder {
	qb.m = true
	qb.q = qb.q + "insert into " + table + " values (" + strings.Join(strings.Split(strings.Repeat("?", len(values)), ""), ",") + ")"
	for _, item := range values {
		qb.v = append(qb.v, fmt.Sprintf("%v", item))
	}
	return qb
}
