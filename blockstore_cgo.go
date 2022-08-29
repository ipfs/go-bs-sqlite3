//go:build cgo

package sqlite3bs

import (
	"database/sql"
	"fmt"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
)

// counter of sqlite drivers registered; guarded by atomic.
var counter int64

// Open creates a new sqlite3-backed blockstore.
func Open(path string, _ Options) (*Blockstore, error) {
	driver := fmt.Sprintf("sqlite3_blockstore_%d", atomic.AddInt64(&counter, 1))
	sql.Register(driver,
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				// Execute pragmas on connection creation.
				for _, p := range pragmas {
					if _, err := conn.Exec(p, nil); err != nil {
						return fmt.Errorf("failed to execute sqlite3 init pragma: %s; err: %w", p, err)
					}
				}
				return nil
			},
		})

	db, err := sql.Open(driver, path+"?mode=rwc")
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite3 database: %w", err)
	}

	// Execute init DDLs.
	for _, ddl := range initDDL {
		if _, err := db.Exec(ddl); err != nil {
			return nil, fmt.Errorf("failed to execute sqlite3 init DDL: %s; err: %w", ddl, err)
		}
	}

	bs := &Blockstore{db: db}

	// Prepare all statements.
	for i, p := range statements {
		if bs.prepared[i], err = db.Prepare(p); err != nil {
			return nil, fmt.Errorf("failed to prepare statement: %s; err: %w", p, err)
		}
	}
	return bs, nil
}
