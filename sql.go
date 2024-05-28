// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlchemy

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"

	"github.com/google/uuid"
	"yunion.io/x/log"
)

var _db *DB

type DB struct {
	DB *sql.DB
}

func (db *DB) SetMaxIdleConns(n int) {
	db.DB.SetMaxIdleConns(n)
}

func (db *DB) SetMaxOpenConns(n int) {
	db.DB.SetMaxOpenConns(n)
}
func (db *DB) Stats() sql.DBStats {
	return db.DB.Stats()
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	line := getFileLine()
	uid := GetUUID()
	log.Infof("Exec: table %s action %s uuid %s", "", "Exec", uid)
	// /*p=DES,f=CreateDisks,i=869323BC4C51*/
	comment := fmt.Sprintf("/* product=tke-market, file=%s, line=%d, identity=%s */", line.GetFile(), line.GetFileLine(), uid)
	sqlstr := comment + query
	return db.DB.ExecContext(context.Background(), sqlstr, args...)
}

func (db *DB) DoExec(action, table, query string, args ...interface{}) (sql.Result, error) {
	line := getFileLine()
	uid := GetUUID()
	table = strings.Trim(table, "`")
	log.Infof("DoExec: table %s action %s uuid %s", table, action, uid)
	// /*p=DES,f=CreateDisks,i=869323BC4C51*/
	comment := fmt.Sprintf("/* product=tke-market, file=%s, line=%d, flow=%s, identity=%s */", line.GetFile(), line.GetFileLine(), action, uid)
	sqlstr := comment + query
	return db.DB.ExecContext(context.Background(), sqlstr, args...)
}

func (db *DB) QueryRow(action, table, query string, args ...interface{}) *sql.Row {
	line := getFileLine()
	uid := GetUUID()
	table = strings.Trim(table, "`")
	log.Infof("QueryRow: table %s action %s uuid %s", table, action, uid)
	// /*p=DES,f=CreateDisks,i=869323BC4C51*/
	comment := fmt.Sprintf("/* product=tke-market, file=%s, line=%d, flow=%s, identity=%s */", line.GetFile(), line.GetFileLine(), action, uid)
	sqlstr := comment + query
	return db.DB.QueryRow(sqlstr, args...)
}

func (db *DB) Query(action, table, query string, args ...interface{}) (*sql.Rows, error) {
	line := getFileLine()
	uid := GetUUID()
	table = strings.Trim(table, "`")
	log.Infof("Query: table %s action %s uuid %s", table, action, uid)
	// /*p=DES,f=CreateDisks,i=869323BC4C51*/
	comment := fmt.Sprintf("/* product=tke-market, file=%s, line=%d, flow=%s, identity=%s */", line.GetFile(), line.GetFileLine(), action, uid)
	sqlstr := comment + query
	return db.DB.Query(sqlstr, args...)
}

func (db *DB) Close() error {
	return db.DB.Close()
}

func SetDB(db *sql.DB) {
	_db = &DB{DB: db}
}

func GetDB() *DB {
	return _db
}

func CloseDB() {
	_db.Close()
	_db = nil
}

type tableName struct {
	Name string
}

func GetTables() []string {
	tables := make([]tableName, 0)
	q := NewRawQuery("SHOW TABLES", "name")
	err := q.All(&tables)
	if err != nil {
		log.Errorf("show tables fail %s", err)
		return nil
	}
	ret := make([]string, len(tables))
	for i, t := range tables {
		ret[i] = t.Name
	}
	return ret
}

type fileLine struct {
	oneCloudFile string
	oneCloudLine int
	marketFile   string
	marketLine   int
}

func (l *fileLine) GetFileLine() int {
	if l.marketFile != "" {
		return l.marketLine
	}
	return l.oneCloudLine
}

func (l *fileLine) String() string {
	return fmt.Sprintf("oneCloudFile  %s:%d marketFile %s:%d", l.oneCloudFile, l.oneCloudLine, l.marketFile, l.marketLine)
}

func (l *fileLine) GetFile() string {
	if l.marketFile != "" {
		return l.marketFile
	}
	return l.oneCloudFile
}

func getFileLine() fileLine {
	const maxStackDepth = 32
	var pcs [maxStackDepth]uintptr
	n := runtime.Callers(0, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	fLine := fileLine{}
	for {
		frame, more := frames.Next()
		// 调用方是onecloud
		if strings.Contains(frame.File, "yunion.io/x/onecloud") && fLine.oneCloudFile == "" {
			fLine.oneCloudFile = frame.File
			fLine.oneCloudLine = frame.Line
		}
		// 调用方包含market并且不包含dispatcher
		if strings.Contains(frame.File, "market") && !strings.Contains(frame.File, "dispatcher") && fLine.marketFile == "" {
			fLine.marketFile = frame.File
			fLine.marketLine = frame.Line
		}
		if !more {
			break
		}
	}
	return fLine
}

func GetUUID() string {
	return uuid.New().String()
}
