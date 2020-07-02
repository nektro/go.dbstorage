package dbstorage

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/nektro/go-util/arrays/stringsu"
	"github.com/nektro/go-util/util"
	"github.com/nektro/go-util/vflag"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/nektro/go-util/alias"
)

// Flags for mysql connection
var (
	flagsMysql = []*string{
		/*0*/ vflag.String("mysql-url", "", ""),
		/*1*/ vflag.String("mysql-user", "", ""),
		/*2*/ vflag.String("mysql-password", "", ""),
		/*3*/ vflag.String("mysql-database", "", ""),
	}
)

type mysqlDB struct {
	db *sql.DB
}

// ConnectMysql does
func ConnectMysql() (Database, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", *flagsMysql[1], *flagsMysql[2], *flagsMysql[0], *flagsMysql[3])
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.New("mysql: sql.Open: " + err.Error())
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(time.Second)
	return &mysqlDB{db}, db.Ping()
}

func (db *mysqlDB) Ping() error {
	return db.db.Ping()
}

func (db *mysqlDB) Close() error {
	return db.db.Close()
}

func (db *mysqlDB) DB() *sql.DB {
	return db.db
}

func (db *mysqlDB) CreateTable(name string, pk []string, columns [][]string) {
	if !db.DoesTableExist(name) {
		db.QueryPrepared(true, F("CREATE TABLE %s(%s %s)", name, pk[0], pk[1]))
		util.Log(F("Created table '%s'", name))
	}
	pti := db.QueryColumnList(name)
	for _, col := range columns {
		if !stringsu.Contains(pti, col[0]) {
			db.QueryPrepared(true, F("ALTER TABLE %s ADD %s %s", name, col[0], col[1]))
		}
	}
}

func (db *mysqlDB) CreateTableStruct(name string, v interface{}) {
	t := reflect.TypeOf(v)
	cols := [][]string{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		g := f.Tag.Get("mysql")
		if len(g) > 0 {
			cols = append(cols, []string{f.Tag.Get("json"), g})
			util.Log(F("Added column '%s.%s'", name, col[0]))
		}
	}
	db.CreateTable(name, []string{"id", "BIGINT NOT NULL PRIMARY KEY"}, cols)
}

func (db *mysqlDB) DoesTableExist(table string) bool {
	q := db.QueryPrepared(false, "SHOW TABLES LIKE '"+table+"'")
	defer q.Close()
	return q.Next()
}

func (db *mysqlDB) QueryColumnList(table string) []string {
	var result []string
	q := db.QueryPrepared(false, "SHOW COLUMNS FROM "+table)
	defer q.Close()
	for q.Next() {
		var name string
		var typ string
		var nul bool
		var key string
		var def string
		var extra string
		q.Scan(&name, &typ, &nul, &key, &def, &extra)
		result = append(result, name)
	}
	return result
}

func (db *mysqlDB) QueryNextID(table string) int64 {
	result := int64(0)
	rows := db.QueryPrepared(false, F("SELECT id FROM %s ORDER BY id DESC LIMIT 1", table))
	for rows.Next() {
		rows.Scan(&result)
	}
	rows.Close()
	return result + 1
}

func (db *mysqlDB) QueryPrepared(modify bool, q string, args ...interface{}) *sql.Rows {
	stmt, err := db.db.Prepare(q)
	if err != nil {
		return nil
	}
	if modify {
		stmt.Exec(args...)
		return nil
	}
	rows, _ := stmt.Query(args...)
	if err != nil {
		return nil
	}
	return rows
}

func (db *mysqlDB) DropTable(name string) {
	db.QueryPrepared(true, "DROP TABLE IF EXISTS "+name)
}

func (db *mysqlDB) QueryRowCount(table string) int64 {
	rows := db.QueryPrepared(false, "SELECT COUNT(*) FROM "+table)
	if rows == nil {
		return -1
	}
	defer rows.Close()
	c := int64(0)
	rows.Next()
	rows.Scan(&c)
	return c
}

//
//

type mysqlQB struct {
	d *mysqlDB       // db
	q string         // query string
	v []driver.Value // values
	m bool           // modify
	w [][4]string    // where's
	o [][2]string    // order's
	l int64          // limit
	f int64          // offset
}

func (db *mysqlDB) Build() QueryBuilder {
	qb := new(mysqlQB)
	qb.d = db
	return qb
}

func (qb *mysqlQB) Se(cols string) QueryBuilder {
	qb.m = false
	qb.q = qb.q + "SELECT " + cols
	return qb
}

func (qb *mysqlQB) Fr(table string) QueryBuilder {
	qb.q = qb.q + " FROM " + table
	return qb
}

func (qb *mysqlQB) WR(col string, op string, value string, raw bool, ags ...interface{}) QueryBuilder {
	qb.w = append(qb.w, [4]string{col, op, value, strconv.FormatBool(raw)})
	for _, item := range ags {
		o, _ := driver.DefaultParameterConverter.ConvertValue(item)
		qb.v = append(qb.v, o)
	}
	return qb
}

func (qb *mysqlQB) Wr(col string, op string, value string) QueryBuilder {
	qb.WR(col, op, value, false)
	return qb
}

func (qb *mysqlQB) Wh(col string, value string) QueryBuilder {
	qb.Wr(col, "=", value)
	return qb
}

func (qb *mysqlQB) Or(col string, order string) QueryBuilder {
	qb.o = append(qb.o, [2]string{col, order})
	return qb
}

func (qb *mysqlQB) Lm(limit int64) QueryBuilder {
	qb.l = limit
	return qb
}

func (qb *mysqlQB) Of(offset int64) QueryBuilder {
	qb.f = offset
	return qb
}

func (qb *mysqlQB) Exe() *sql.Rows {
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
				qb.q += " WHERE " + item[0] + " " + item[1] + " ?"
			} else {
				qb.q += " AND " + item[0] + " " + item[1] + " ?"
			}
			vals = append(vals, item[2])
		} else {
			if i == 0 {
				qb.q += " WHERE " + item[0] + " " + item[1] + " " + item[2]
			} else {
				qb.q += " AND " + item[0] + " " + item[1] + " " + item[2]
			}
		}
	}
	for i, item := range qb.o {
		if i == 0 {
			qb.q += " ORDER BY " + item[0] + " " + item[1]
		} else {
			qb.q += ", " + item[0] + " " + item[1]
		}
	}
	if qb.l > 0 {
		qb.q += " LIMIT " + strconv.FormatInt(qb.l, 10)

		if qb.f > 0 {
			qb.q += " OFFSET " + strconv.FormatInt(qb.f, 10)
		}
	}
	iva := make([]interface{}, len(vals))
	for i, v := range vals {
		iva[i] = v
	}
	return qb.d.QueryPrepared(qb.m, qb.q, iva...)
}

func (qb *mysqlQB) Up(table string, col string, value string) QueryBuilder {
	qb.m = true
	qb.q = qb.q + "UPDATE " + table + " SET " + col + " = ?"
	qb.v = append(qb.v, value)
	return qb
}

func (qb *mysqlQB) Ins(table string, values ...interface{}) Executable {
	qb.m = true
	qb.q = qb.q + "INSERT INTO " + table + " VALUES (" + strings.Join(strings.Split(strings.Repeat("?", len(values)), ""), ",") + ")"
	for _, item := range values {
		o, _ := driver.DefaultParameterConverter.ConvertValue(item)
		qb.v = append(qb.v, o)
	}
	return qb
}

func (qb *mysqlQB) InsI(table string, strct interface{}) Executable {
	v := reflect.ValueOf(strct).Elem()
	t := v.Type()
	atrs := []interface{}{}
	for i := 0; i < t.NumField(); i++ {
		sv := v.Field(i).Interface()
		atrs = append(atrs, sv)
	}
	return qb.Ins(table, atrs...)
}

func (qb *mysqlQB) Del(table string) QueryBuilder {
	qb.m = true
	qb.q = "DELETE FROM " + table
	return qb
}
