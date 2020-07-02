package dbstorage

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/nektro/go-util/util"

	. "github.com/nektro/go-util/alias"
)

// Database represents an active db connection
type Database interface {
	Inner
	CreateTableStruct(name string, v interface{})
}

type Inner interface {
	Ping() error
	Close() error
	DB() *sql.DB
	CreateTable(name string, pk []string, columns [][]string)
	DoesTableExist(table string) bool
	Build() QueryBuilder
	QueryColumnList(table string) []string
	QueryNextID(table string) int64
	QueryRowCount(table string) int64
	DropTable(name string)
	DriverName() string
	TagName() string
	IntPrimaryKey() string
	TypeForType(reflect.Type) string
}

type Outer struct {
	Inner
}

func (db *Outer) CreateTableStruct(name string, v interface{}) {
	t := reflect.TypeOf(v)
	vv := reflect.ValueOf(v)
	cols := [][]string{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		ftj := f.Tag.Get("json")
		g := f.Tag.Get(db.TagName())
		if len(g) > 0 {
			cols = append(cols, []string{ftj, g})
		}
		if len(f.Tag.Get("dbsorm")) > 0 {
			vfi := vv.Field(i).Type()
			g := db.TypeForType(vfi)
			if len(g) > 0 {
				cols = append(cols, []string{ftj, g})
				fmt.Println("dbsorm:", ftj, g)
				continue
			}
			util.DieOnError(E("dbstorage: unknown struct field type:"), F("%v", vfi), ftj)
		}
	}
	db.CreateTable(name, []string{"id", db.IntPrimaryKey()}, cols)
}
