package dbstorage

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/nektro/go-util/arrays/stringsu"
	"github.com/nektro/go-util/util"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/nektro/go-util/alias"
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

func ConnectSqlite(path string) (Database, error) {
	op := url.Values{}
	op.Add("mode", "rwc")
	op.Add("cache", "shared")
	op.Add("_busy_timeout", "5000")
	op.Add("_journal_mode", "OFF")
	db, err := sql.Open("sqlite3", "file:"+path+"?"+op.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &DbProxy{db}, db.Ping()
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
		db.QueryPrepared(true, F("create table %s(%s %s)", name, pk[0], pk[1]))
		util.Log(F("Created table '%s'", name))
	}
	pti := db.QueryColumnList(name)
	for _, col := range columns {
		if !stringsu.Contains(pti, col[0]) {
			db.QueryPrepared(true, F("alter table %s add %s %s", name, col[0], col[1]))
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
	q := db.QueryPrepared(false, F("select name from sqlite_master where type='table' AND name='%s';", table))
	e := q.Next()
	q.Close()
	return e
}

func (db *DbProxy) QueryTableInfo(table string) []PragmaTableInfo {
	var result []PragmaTableInfo
	rows := db.QueryPrepared(false, F("pragma table_info(%s)", table))
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
	rows := db.QueryPrepared(false, F("select id from %s order by id desc limit 1", table))
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

func (db *DbProxy) DropTable(name string) {
	db.QueryPrepared(true, "drop table if exists "+name)
}

func (db *DbProxy) QueryRowCount(table string) int64 {
	rows := db.Build().Se("count(*)").Fr(table).Exe()
	defer rows.Close()
	if !rows.Next() {
		return -1
	}
	c := int64(0)
	rows.Scan(&c)
	return c
}

//

//

type sQueryBuilder struct {
	d *DbProxy       // db
	q string         // query string
	v []driver.Value // values
	m bool           // modify
	w [][4]string    // where's
	o [][2]string    // order's
	l int64          // limit
	f int64          // offset
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

func (qb *sQueryBuilder) WR(col string, op string, value string, raw bool, ags ...interface{}) QueryBuilder {
	qb.w = append(qb.w, [4]string{col, op, value, strconv.FormatBool(raw)})
	for _, item := range ags {
		o, _ := driver.DefaultParameterConverter.ConvertValue(item)
		qb.v = append(qb.v, o)
	}
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
	for _, item := range qb.v {
		if b, ok := item.(bool); ok {
			vals = append(vals, strconv.Itoa(util.Btoi(b)))
			continue
		}
		vals = append(vals, fmt.Sprintf("%v", item))
	}
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

func (qb *sQueryBuilder) Ins(table string, values ...interface{}) Executable {
	qb.m = true
	qb.q = qb.q + "insert into " + table + " values (" + strings.Join(strings.Split(strings.Repeat("?", len(values)), ""), ",") + ")"
	for _, item := range values {
		o, _ := driver.DefaultParameterConverter.ConvertValue(item)
		qb.v = append(qb.v, o)
	}
	return qb
}

func (qb *sQueryBuilder) InsI(table string, strct interface{}) Executable {
	v := reflect.ValueOf(strct).Elem()
	t := v.Type()
	atrs := []interface{}{}
	for i := 0; i < t.NumField(); i++ {
		sv := v.Field(i).Interface()
		atrs = append(atrs, sv)
	}
	return qb.Ins(table, atrs...)
}

func (qb *sQueryBuilder) Del(table string) QueryBuilder {
	qb.m = true
	qb.q = "delete from " + table
	return qb
}
