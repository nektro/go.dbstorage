package dbstorage

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/nektro/go-util/arrays/stringsu"
	"github.com/nektro/go-util/util"
	"github.com/nektro/go-util/vflag"

	_ "github.com/lib/pq"
	. "github.com/nektro/go-util/alias"
)

// Flags for postgres connection
var (
	flagsPostgres = []*string{
		/*0*/ vflag.String("postgres-url", "", ""),
		/*1*/ vflag.String("postgres-user", "", ""),
		/*2*/ vflag.String("postgres-password", "", ""),
		/*3*/ vflag.String("postgres-database", "", ""),
		/*4*/ vflag.String("postgres-sslmode", "verify-full", ""),
	}
)

type postgresDB struct {
	db *sql.DB
}

// ConnectPostgres does
func ConnectPostgres() (Database, error) {
	op := url.Values{}
	op.Add("user", *flagsPostgres[1])
	op.Add("password", *flagsPostgres[2])
	op.Add("dbname", *flagsPostgres[3])
	op.Add("sslmode", *flagsPostgres[4])
	op.Add("connect_timeout", "5")
	dsn := "postgres://" + *flagsPostgres[0] + "/?" + op.Encode()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, errors.New("postgres: sql.Open: " + err.Error())
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(time.Second)
	return &Outer{&postgresDB{db}}, db.Ping()
}

func (db *postgresDB) Ping() error {
	return db.db.Ping()
}

func (db *postgresDB) Close() error {
	return db.db.Close()
}

func (db *postgresDB) DB() *sql.DB {
	return db.db
}

func (db *postgresDB) DriverName() string {
	return "postgres"
}

func (db *postgresDB) TagName() string {
	return "postgres"
}

func (db *postgresDB) IntPrimaryKey() string {
	return "BIGINT PRIMARY KEY NOT NULL"
}

func (db *postgresDB) TypeForType(t reflect.Type) string {
	switch t.Name() {
	case "string":
		return "text"
	case "bool", "int8", "int16":
		return "smallint"
	case "int", "int32":
		return "int"
	case "int64":
		return "bigint"
	case "float32":
		return "real"
	case "float64":
		return "double precision"
	}
	dv, ok := reflect.New(t).Interface().(driver.Valuer)
	if ok {
		v, _ := dv.Value()
		return db.TypeForType(reflect.TypeOf(v))
	}
	return ""
}

func (db *postgresDB) CreateTable(name string, pk []string, columns [][]string) {
	if !db.DoesTableExist(name) {
		db.QueryPrepared(true, F("CREATE TABLE %s(%s %s)", name, pk[0], pk[1]))
		util.Log(F("Created table '%s'", name))
	}
	pti := db.QueryColumnList(name)
	for _, col := range columns {
		if !stringsu.Contains(pti, col[0]) {
			db.QueryPrepared(true, F("ALTER TABLE %s ADD COLUMN %s %s", name, col[0], col[1]))
			util.Log(F("Added column '%s.%s'", name, col[0]))
		}
	}
}

func (db *postgresDB) DoesTableExist(table string) bool {
	table = strings.ToLower(table)
	// https://www.postgresql.org/docs/9.5/infoschema-tables.html
	q := db.QueryPrepared(false, F("SELECT * FROM information_schema.tables WHERE table_name = '%s'", table))
	if q == nil {
		return false
	}
	defer q.Close()
	return q.Next()
}

func (db *postgresDB) QueryColumnList(table string) []string {
	table = strings.ToLower(table)
	var result []string
	// https://www.postgresql.org/docs/9.5/infoschema-columns.html
	rows := db.QueryPrepared(false, F("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table))
	defer rows.Close()
	for rows.Next() {
		var v string
		rows.Scan(&v)
		result = append(result, v)
	}
	return result
}

func (db *postgresDB) QueryNextID(table string) int64 {
	result := int64(0)
	rows := db.QueryPrepared(false, F("SELECT id FROM %s ORDER BY id DESC LIMIT 1", table))
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&result)
	}
	return result + 1
}

func (db *postgresDB) QueryPrepared(modify bool, q string, args ...interface{}) *sql.Rows {
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

func (db *postgresDB) DropTable(name string) {
	db.QueryPrepared(true, "DROP TABLE IF EXISTS "+name)
}

func (db *postgresDB) QueryRowCount(table string) int64 {
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

type postgresQB struct {
	d *postgresDB    // db
	q string         // query string
	v []driver.Value // values
	m bool           // modify
	w [][4]string    // where's
	o [][2]string    // order's
	l int64          // limit
	f int64          // offset
}

func (db *postgresDB) Build() QueryBuilder {
	qb := new(postgresQB)
	qb.d = db
	return qb
}

func (qb *postgresQB) Se(cols string) QueryBuilder {
	qb.m = false
	qb.q = qb.q + "SELECT " + cols
	return qb
}

func (qb *postgresQB) Fr(table string) QueryBuilder {
	qb.q = qb.q + " FROM " + table
	return qb
}

func (qb *postgresQB) WR(col string, op string, value string, raw bool, ags ...interface{}) QueryBuilder {
	qb.w = append(qb.w, [4]string{col, op, value, strconv.FormatBool(raw)})
	for _, item := range ags {
		o, _ := driver.DefaultParameterConverter.ConvertValue(item)
		qb.v = append(qb.v, o)
	}
	return qb
}

func (qb *postgresQB) Wr(col string, op string, value string) QueryBuilder {
	qb.WR(col, op, value, false)
	return qb
}

func (qb *postgresQB) Wh(col string, value string) QueryBuilder {
	qb.Wr(col, "=", value)
	return qb
}

func (qb *postgresQB) Or(col string, order string) QueryBuilder {
	qb.o = append(qb.o, [2]string{col, order})
	return qb
}

func (qb *postgresQB) Lm(limit int64) QueryBuilder {
	qb.l = limit
	return qb
}

func (qb *postgresQB) Of(offset int64) QueryBuilder {
	qb.f = offset
	return qb
}

func (qb *postgresQB) Exe() *sql.Rows {
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
	qcnt := strings.Count(qb.q, "?")
	for i := 1; i <= qcnt; i++ {
		qb.q = strings.Replace(qb.q, "?", "$"+strconv.Itoa(i), 1)
	}
	return qb.d.QueryPrepared(qb.m, qb.q, iva...)
}

func (qb *postgresQB) Up(table string, col string, value string) QueryBuilder {
	qb.m = true
	qb.q = qb.q + "UPDATE " + table + " SET " + col + " = ?"
	qb.v = append(qb.v, value)
	return qb
}

func (qb *postgresQB) Ins(table string, values ...interface{}) Executable {
	qb.m = true
	qb.q = qb.q + "INSERT INTO " + table + " VALUES (" + strings.Join(strings.Split(strings.Repeat("?", len(values)), ""), ",") + ")"
	for _, item := range values {
		o, _ := driver.DefaultParameterConverter.ConvertValue(item)
		qb.v = append(qb.v, o)
	}
	return qb
}

func (qb *postgresQB) InsI(table string, strct interface{}) Executable {
	v := reflect.ValueOf(strct).Elem()
	t := v.Type()
	atrs := []interface{}{}
	for i := 0; i < t.NumField(); i++ {
		sv := v.Field(i).Interface()
		atrs = append(atrs, sv)
	}
	return qb.Ins(table, atrs...)
}

func (qb *postgresQB) Del(table string) QueryBuilder {
	qb.m = true
	qb.q = "DELETE FROM " + table
	return qb
}
