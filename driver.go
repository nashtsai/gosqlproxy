// Copyright 2014 The Go SQL Proxy Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gosqlproxy

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
)

// type Driver interface {
//         // Open returns a new connection to the database.
//         // The name is a string in a driver-specific format.
//         //
//         // Open may return a cached connection (one previously
//         // closed), but doing so is unnecessary; the sql package
//         // maintains a pool of idle connections for efficient re-use.
//         //
//         // The returned connection is only used by one goroutine at a
//         // time.
//         Open(name string) (Conn, error)
// }

// type Conn interface {
//         // Prepare returns a prepared statement, bound to this connection.
//         Prepare(query string) (Stmt, error)

//         // Close invalidates and potentially stops any current
//         // prepared statements and transactions, marking this
//         // connection as no longer in use.
//         //
//         // Because the sql package maintains a free pool of
//         // connections and only calls Close when there's a surplus of
//         // idle connections, it shouldn't be necessary for drivers to
//         // do their own connection caching.
//         Close() error

//         // Begin starts and returns a new transaction.
//         Begin() (Tx, error)
// }

// type Execer interface {
//         Exec(query string, args []Value) (Result, error)
// }

// type Stmt interface {
//         // Close closes the statement.
//         //
//         // As of Go 1.1, a Stmt will not be closed if it's in use
//         // by any queries.
//         Close() error

//         // NumInput returns the number of placeholder parameters.
//         //
//         // If NumInput returns >= 0, the sql package will sanity check
//         // argument counts from callers and return errors to the caller
//         // before the statement's Exec or Query methods are called.
//         //
//         // NumInput may also return -1, if the driver doesn't know
//         // its number of placeholders. In that case, the sql package
//         // will not sanity check Exec or Query argument counts.
//         NumInput() int

//         // Exec executes a query that doesn't return rows, such
//         // as an INSERT or UPDATE.
//         Exec(args []Value) (Result, error)

//         // Exec executes a query that may return rows, such as a
//         // SELECT.
//         Query(args []Value) (Rows, error)
// }

// type Result interface {
//         // LastInsertId returns the database's auto-generated ID
//         // after, for example, an INSERT into a table with primary
//         // key.
//         LastInsertId() (int64, error)

//         // RowsAffected returns the number of rows affected by the
//         // query.
//         RowsAffected() (int64, error)
// }

// type Rows interface {
//         // Columns returns the names of the columns. The number of
//         // columns of the result is inferred from the length of the
//         // slice.  If a particular column name isn't known, an empty
//         // string should be returned for that entry.
//         Columns() []string

//         // Close closes the rows iterator.
//         Close() error

//         // Next is called to populate the next row of data into
//         // the provided slice. The provided slice will be the same
//         // size as the Columns() are wide.
//         //
//         // The dest slice may be populated only with
//         // a driver Value type, but excluding string.
//         // All string values must be converted to []byte.
//         //
//         // Next should return io.EOF when there are no more rows.
//         Next(dest []Value) error
// }

type role int

const (
	masterRole role = iota
	slaveRole
)

var (
	dnsTranslators              map[string]func(*url.URL) string
	masterCounter, slaveCounter uint32
	enableDebug                 = false
)

type ProxyDriver struct {
	dbNameHandlesMap map[string]map[role][]*sql.DB
}

type ProxyConn struct {
	dbHandlesMap *map[role][]*sql.DB
	tx           *sql.Tx
	stmt         *ProxyStmt
}

type ProxyStmt struct {
	conn       *ProxyConn
	stmt       *sql.Stmt
	inputCount int
}

// type ProxyResult struct {
// 	stmt *ProxyStmt
// }

type ProxyRows struct {
	stmt *ProxyStmt
	rows *sql.Rows
}

type ProxyExecer struct {
	// driver *ProxyDriver
	// conn   *ProxyConn
}

func init() {
	sql.Register("gosqlproxy", &ProxyDriver{})
}

func Init() {

	knownDrivers := map[string]func(*url.URL) string{
		"postgres": func(url *url.URL) string {
			password, _ := url.User.Password()
			return fmt.Sprintf("user=%s password=%s dbname=%s %s", url.User.Username(), password, strings.TrimPrefix(url.Path, "/"), url.RawQuery)
		},
		"sqlite3": func(url *url.URL) string { return url.Path },
	}

	for driverName, v := range knownDrivers {
		_, err := sql.Open(driverName, "")
		if err == nil {
			RegisterDSNTranslator(driverName, v)
		} else {
			// fmt.Printf("driver failed: %v | err: %v\n", driverName, err)
		}
	}
}

// Debug prints a debug information to the log with file and line.
func DebugLog(format string, a ...interface{}) {
	if enableDebug {
		_, file, line, _ := runtime.Caller(1)
		info := fmt.Sprintf(format, a...)

		log.Printf("[gosqlproxy] debug %s:%d %v", file, line, info)
	}
}

func InfoLog(format string, a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)

	log.Printf("[gosqlproxy] info %s:%d %v", file, line, info)
}

func ErrorLog(format string, a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)

	log.Printf("[gosqlproxy] info %s:%d %v", file, line, info)
}

func RegisterDSNTranslator(driverName string, translator func(*url.URL) string) (err error) {
	driverName = strings.TrimSpace(driverName)
	if len(driverName) == 0 {
		err = errors.New("driver name is empty")
		return
	}
	if dnsTranslators == nil {
		dnsTranslators = make(map[string]func(*url.URL) string, 0)
	}
	dnsTranslators[driverName] = translator
	return
}

// Only accept DSN common format (http://pear.php.net/manual/en/package.database.db.intro-dsn.php), multiple data sources separated by ';'
// mysql://[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN];mysql://[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]#slave
func (d *ProxyDriver) Open(name string) (driver.Conn, error) {
	DebugLog("ProxyDriver.Open: %v | name:%v", d, name)
	if d.dbNameHandlesMap == nil {
		d.dbNameHandlesMap = make(map[string]map[role][]*sql.DB, 0)
	}

	dbHandlesMap, has := d.dbNameHandlesMap[name]
	if !has {
		dbHandlesMap = make(map[role][]*sql.DB, 0)
		d.dbNameHandlesMap[name] = dbHandlesMap
		dsns := strings.Split(name, ";")
		for _, i := range dsns {
			urlS, err := url.Parse(i)
			if err != nil {
				d.cleanup(name)
				return nil, err
			}
			var dataSourceName string
			driverName := urlS.Scheme

			if len(urlS.Fragment) > 0 {
				if !(urlS.Fragment == "master" || urlS.Fragment == "slave") {
					d.cleanup(name)
					return nil, errors.New("unknown role type: " + urlS.Fragment)
				}
			}

			if dnsTranslators != nil {
				if translator, has := dnsTranslators[urlS.Scheme]; has {
					dataSourceName = translator(urlS)
					DebugLog("translated from: [%s] to [%s]", i, dataSourceName)
				}
			}
			if len(dataSourceName) == 0 {
				dataSourceName = urlS.RequestURI()
				DebugLog("url from: [%s] to [%s]", i, dataSourceName)
			}
			InfoLog("DNS: [%v] | open DSN: [%v] | driver: %v", i, dataSourceName, driverName)

			db, err := sql.Open(driverName, dataSourceName)
			if err != nil {
				d.cleanup(name)
				return nil, err
			}
			var dbHandles []*sql.DB
			var has bool
			role := masterRole
			if urlS.Fragment == "slave" {
				role = slaveRole
			}
			if dbHandles, has = dbHandlesMap[role]; !has {
				dbHandles = make([]*sql.DB, 0)
				dbHandlesMap[role] = dbHandles
			}
			dbHandlesMap[role] = append(dbHandles, db)
		}
	}

	return &ProxyConn{dbHandlesMap: &dbHandlesMap}, nil
}

func (d *ProxyDriver) cleanup(name string) {
	if dbHandlesMap, has := d.dbNameHandlesMap[name]; has {
		for _, v := range dbHandlesMap {
			for _, vv := range v {
				vv.Close()
			}
		}
		delete(d.dbNameHandlesMap, name)
	}
}

func (c *ProxyConn) Prepare(query string) (driver.Stmt, error) {
	DebugLog("ProxyConn.Prepare: %v | query: %v", c, query)
	queryLower := strings.ToLower(query)

	var db *sql.DB
	var dbHandles []*sql.DB
	var has bool
	var dbHandleSize int
	var stepping *uint32
	if strings.HasPrefix(queryLower, "select ") {
		stepping = &slaveCounter
		dbHandles, has = (*c.dbHandlesMap)[slaveRole]
		dbHandleSize = len(dbHandles)
		if has && dbHandleSize == 0 {
			dbHandles = (*c.dbHandlesMap)[masterRole] // using master's db handles if no slave db provided
			dbHandleSize = len(dbHandles)
			stepping = &masterCounter
		} else {
			dbHandles = (*c.dbHandlesMap)[masterRole]
			dbHandleSize = len(dbHandles)
			stepping = &masterCounter
		}
		if dbHandleSize == 0 {
			return nil, errors.New("has no opened DB, how could this happen!?")
		}
	} else {
		dbHandles, has = (*c.dbHandlesMap)[masterRole]
		DebugLog("dbHandles:%v | has: %t", dbHandles, has)
		dbHandleSize = len(dbHandles)
		if !has || dbHandleSize == 0 {
			return nil, errors.New("ster DB, cannot proceed SQL write operation: " + query)
		}
		stepping = &masterCounter
	}

	if dbHandleSize == 1 {
		db = dbHandles[0]
	} else {
		db = dbHandles[atomic.AddUint32(stepping, 1)%uint32(dbHandleSize)]
	}
	DebugLog("dbHandleSize:%v, db:%v, query:%s", dbHandleSize, db, query)

	sqlStmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	} else {
		return &ProxyStmt{conn: c, stmt: sqlStmt, inputCount: strings.Count(query, "?")}, err
	}
}

func (c *ProxyConn) Close() (err error) {
	DebugLog("ProxyConn.Close: %v | enter", c)
	// !nashtsai! still leave

	if c.tx != nil {
		// !nashtsai! should I commit tx?
		c.tx = nil
	}

	if c.stmt != nil {
		err = c.stmt.Close()
		c.stmt = nil
	}
	return
}

func (c *ProxyConn) Begin() (tx driver.Tx, err error) {
	DebugLog("ProxyConn.Begin: %v | enter", c)
	var db *sql.DB
	dbHandles, has := (*c.dbHandlesMap)[masterRole]
	dbHandleSize := len(dbHandles)
	if !has || dbHandleSize == 0 {
		return nil, errors.New("has no master DB, cannot BEGIN a TX")
	}
	stepping := &masterCounter
	if dbHandleSize == 1 {
		db = dbHandles[0]
	} else {
		db = dbHandles[atomic.AddUint32(stepping, 1)%uint32(dbHandleSize)]
	}
	c.tx, err = db.Begin()
	tx = c.tx
	return
}

func (s *ProxyStmt) Close() (err error) {
	DebugLog("ProxyStmt.Close: %v | enter", s)
	if s.stmt != nil {
		err = s.stmt.Close()
		s.stmt = nil
	}
	s.inputCount = 0
	return
}

func (s *ProxyStmt) NumInput() int {
	DebugLog("ProxyStmt.NumInput: %v | enter | NumInput:%v", s, s.inputCount)
	return s.inputCount
}

func values2InterfaceArray(args []driver.Value) []interface{} {
	DebugLog("values2InterfaceArray enter:%v, len:%v", args, len(args))
	forwardArgs := make([]interface{}, cap(args))
	for idx, i := range args {
		val := reflect.ValueOf(i)
		DebugLog("value:%v, CanAddr:%t, Kind:%v, IsValid:%t", val, val.CanAddr(), val.Kind(), val.IsValid())
		if val.IsValid() {
			forwardArgs[idx] = i
		} else {
			var arg interface{}
			forwardArgs[idx] = &arg
		}
	}
	return forwardArgs
}

func (s *ProxyStmt) Exec(args []driver.Value) (result driver.Result, err error) {
	DebugLog("ProxyStmt.Exec: %v | enter", s)
	s.inputCount = len(args)
	return s.stmt.Exec(values2InterfaceArray(args)...)
}

func (s *ProxyStmt) Query(args []driver.Value) (driver.Rows, error) {
	DebugLog("ProxyStmt.Query: %v | enter", s)
	s.inputCount = len(args)
	sqlRows, err := s.stmt.Query(values2InterfaceArray(args)...)
	if err == nil {
		return &ProxyRows{stmt: s, rows: sqlRows}, err
	}
	return nil, err
}

func (r *ProxyRows) Columns() []string {
	DebugLog("ProxyRows.Columns: %v | enter", r)

	columns, err := r.rows.Columns()
	if err != nil {
		panic(err.Error())
	}
	DebugLog("ProxyRows.Columns: %v | columns:%v", r, columns)
	return columns
}

func (r *ProxyRows) Close() (err error) {
	DebugLog("ProxyRows.Close: %v | enter", r)
	if r.rows != nil {
		err = r.rows.Close()
		r.rows = nil
	}
	return
}

// type myInterface interface{}
// type customslice []driver.Value
// type customslice1 []myInterface
// type customslice2 []interface{}

func (r *ProxyRows) Next(dest []driver.Value) error {
	DebugLog("ProxyRows.Next: %v | enter", r)
	if !r.rows.Next() {
		return io.EOF
	}

	dest1 := values2InterfaceArray(dest)

	if err := r.rows.Scan(dest1...); err != nil {
		panic(err.Error())
	}

	for idx, i := range dest1 {
		dest[idx] = reflect.Indirect(reflect.ValueOf(i)).Interface()
	}

	return nil
}
