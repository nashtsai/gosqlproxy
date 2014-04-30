package gosqlproxy

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
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
	MASTER_ROLE role = iota
	SLAVE_ROLE
)

var (
	dnsTranslators              map[string]func(*url.URL) string
	masterCounter, slaveCounter uint32
)

type ProxyDriver struct {
	dbHandlesMap map[role][]*sql.DB
}

type ProxyConn struct {
	driver *ProxyDriver
	tx     *sql.Tx
	stmt   *ProxyStmt
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

// Debug prints a debug information to the log with file and line.
func Debug(format string, a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)

	log.Printf("[gosqlproxy] debug %s:%d %v", file, line, info)
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
// mysql:[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN];mysql:[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]#slave
func (d *ProxyDriver) Open(name string) (driver.Conn, error) {
	Debug("name:%v", name)
	if d.dbHandlesMap == nil {
		d.dbHandlesMap = make(map[role][]*sql.DB, 0)
		dsns := strings.Split(name, ";")
		for _, i := range dsns {
			urlS, err := url.Parse(i)
			var dataSourceName string
			driverName := urlS.Scheme
			fragment := urlS.Fragment

			if len(fragment) > 0 {
				if !(fragment == "master" || fragment == "slave") {
					d.cleanup()
					return nil, errors.New("unknown role type: " + fragment)
				}
			}

			if dnsTranslators != nil {
				if translator, has := dnsTranslators[urlS.Scheme]; has {
					dataSourceName = translator(urlS)
				} else {
					urlS.Scheme = ""
					urlS.Fragment = ""
					dataSourceName = urlS.String()
				}
			} else {
				urlS.Scheme = ""
				urlS.Fragment = ""

				dataSourceName = urlS.String()
			}
			Debug("real dataSourceName: %v | driver: %v", dataSourceName, driverName)

			db, err := sql.Open(driverName, dataSourceName)
			if err != nil {
				d.cleanup()
				return nil, err
			}
			var dbHandles []*sql.DB
			var has bool
			role := MASTER_ROLE
			if fragment == "slave" {
				role = SLAVE_ROLE
			}
			if dbHandles, has = d.dbHandlesMap[role]; !has {
				dbHandles = make([]*sql.DB, 0)
				d.dbHandlesMap[role] = dbHandles
			}
			d.dbHandlesMap[role] = append(dbHandles, db)
		}
	}

	return &ProxyConn{driver: d}, nil
}

func (d *ProxyDriver) cleanup() {
	d.dbHandlesMap = nil
}

func (c *ProxyConn) Prepare(query string) (driver.Stmt, error) {
	Debug("enter")
	queryLower := strings.ToLower(query)

	var db *sql.DB
	var dbHandles []*sql.DB
	var has bool
	var dbHandleSize int
	var stepping *uint32
	if strings.HasPrefix(queryLower, "select ") {
		stepping = &slaveCounter
		dbHandles, has = c.driver.dbHandlesMap[SLAVE_ROLE]
		dbHandleSize = len(dbHandles)
		if has && dbHandleSize == 0 {
			dbHandles = c.driver.dbHandlesMap[MASTER_ROLE] // using master's db handles if no slave db provided
			dbHandleSize = len(dbHandles)
			stepping = &masterCounter
		} else {
			dbHandles = c.driver.dbHandlesMap[MASTER_ROLE]
			dbHandleSize = len(dbHandles)
			stepping = &masterCounter
		}
		if dbHandleSize == 0 {
			return nil, errors.New("has no opened DB, how could this happen!?")
		}
	} else {
		dbHandles, has = c.driver.dbHandlesMap[MASTER_ROLE]
		Debug("dbHandles:%v | has: %t", dbHandles, has)
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
	Debug("dbHandleSize:%v, db:%v", dbHandleSize, db)

	sqlStmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	} else {
		return &ProxyStmt{conn: c, stmt: sqlStmt}, err
	}
}

func (c *ProxyConn) Close() (err error) {
	Debug("enter")
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
	Debug("enter")
	var db *sql.DB
	dbHandles, has := c.driver.dbHandlesMap[MASTER_ROLE]
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

func (s *ProxyStmt) Close() error {
	Debug("enter")
	if s.stmt != nil {
		s.stmt.Close()
	}
	s.inputCount = 0
	return nil
}

func (s *ProxyStmt) NumInput() int {
	Debug("enter")
	return s.inputCount
}

func values2InterfaceArray(args []driver.Value) []interface{} {
	forwardArgs := make([]interface{}, len(args))
	for idx, i := range args {
		forwardArgs[idx] = i
	}
	return forwardArgs
}

func (s *ProxyStmt) Exec(args []driver.Value) (result driver.Result, err error) {
	Debug("enter")
	s.inputCount = len(args)
	return s.stmt.Exec(values2InterfaceArray(args)...)
}

func (s *ProxyStmt) Query(args []driver.Value) (driver.Rows, error) {
	Debug("enter")
	s.inputCount = len(args)
	sqlRows, err := s.stmt.Query(values2InterfaceArray(args)...)
	if err != nil {
		return &ProxyRows{stmt: s, rows: sqlRows}, err
	}
	return nil, err
}

func (r *ProxyRows) Columns() []string {
	Debug("enter")
	columns, err := r.rows.Columns()
	if err != nil {
		panic(err.Error())
	}
	return columns
}

func (r *ProxyRows) Close() error {
	return r.rows.Close()
}

// type myInterface interface{}
// type customslice []driver.Value
// type customslice1 []myInterface
// type customslice2 []interface{}

func (r *ProxyRows) Next(dest []driver.Value) error {
	Debug("enter")
	if !r.rows.Next() {
		return io.EOF
	}

	dest1 := make([]interface{}, len(dest))
	r.rows.Scan(dest1...)

	for idx, i := range dest1 {
		dest[idx] = i
	}

	return nil
}
