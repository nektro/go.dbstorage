package dbstorage_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/nektro/go-util/util"
	"github.com/nektro/go-util/vflag"
	dbstorage "github.com/nektro/go.dbstorage"
	"github.com/nektro/go.etc/dbt"
)

const (
	TableName   = "New_Tablee"
	letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type TestRow struct {
	ID       int64    `json:"id"`
	Name     string   `json:"name" dbsorm:"1"`
	Admin    bool     `json:"admin" dbsorm:"1"`
	Age      int      `json:"age" dbsorm:"1"`
	Birthday dbt.Time `json:"birthday" dbsorm:"1"`
}

func RandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func DoTest(t *testing.T, db dbstorage.Database) {
	rand.Seed(time.Now().UnixNano())
	// t.Log(db.DB())

	t.Log(db.DoesTableExist(TableName))
	t.Log(db.QueryRowCount(TableName))
	db.CreateTableStruct(TableName, TestRow{})
	t.Log(db.DoesTableExist(TableName))
	t.Log(db.QueryColumnList(TableName))
	t.Log(db.QueryRowCount(TableName))

	for i := 0; i < 500; i++ {
		dbstorage.InsertsLock.Lock()
		id := db.QueryNextID(TableName)
		nr := &TestRow{id, RandomString(12), id == 1, rand.Intn(25), dbt.Time(time.Now())}
		db.Build().InsI(TableName, nr).Exe()
		dbstorage.InsertsLock.Unlock()
	}
	t.Log(db.QueryRowCount(TableName))

	db.Build().Del(TableName).Wh("age", "12").Exe()
	t.Log(db.QueryRowCount(TableName))

	db.Build().Up(TableName, "admin", "1").Wh("admin", "0").Wh("age", "12").Exe()

	db.Build().Se("*").Fr(TableName).Wh("age", "14").Lm(25).Exe().Close()

	db.Build().Se("*").Fr(TableName).Wh("age", "20").Lm(10).Of(10).Exe().Close()

	db.Build().Se("*").Fr(TableName).Wh("name", "meghan").Exe().Close()

	db.DropTable(TableName)
	t.Log(db.QueryRowCount(TableName))

	db.Close()
}

//
//
//

func init() {
	vflag.Parse()
}

func TestSqlite(t *testing.T) {
	d, err := dbstorage.ConnectSqlite("./.test.db")
	util.DieOnError(err)
	DoTest(t, d)
}

func TestPostgres(t *testing.T) {
	d, err := dbstorage.ConnectPostgres()
	util.DieOnError(err)
	DoTest(t, d)
}

func TestMysql(t *testing.T) {
	d, err := dbstorage.ConnectMysql()
	util.DieOnError(err)
	DoTest(t, d)
}
