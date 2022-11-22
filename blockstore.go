package sqlite3bs

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
)

// pragmas are sqlite pragmas to be applied at initialization.
var pragmas = []string{
	fmt.Sprintf("PRAGMA busy_timeout = %d", 10*1000), // milliseconds
	"PRAGMA synchronous = normal",
	"PRAGMA temp_store = memory",
	"PRAGMA mmap_size = 30000000000",
	"PRAGMA page_size = 32768",
	"PRAGMA auto_vacuum = NONE",
	"PRAGMA automatic_index = OFF",
	"PRAGMA journal_mode = WAL",
	"PRAGMA read_uncommitted = ON",
}

var initDDL = []string{
	// spacing not important
	`CREATE TABLE IF NOT EXISTS blocks (
		mh TEXT NOT NULL PRIMARY KEY,
		codec INTEGER NOT NULL,
		bytes BLOB NOT NULL
	) WITHOUT ROWID`,

	// placeholder version to enable migrations.
	`CREATE TABLE IF NOT EXISTS _meta (
    	version UINT64 NOT NULL UNIQUE
	)`,

	// version 1.
	`INSERT OR IGNORE INTO _meta (version) VALUES (1)`,
}

const (
	stmtHas = iota
	stmtGet
	stmtGetSize
	stmtPut
	stmtDelete
	stmtSelectAll
)

// statements are statements to prepare.
var statements = [...]string{
	stmtHas:       "SELECT EXISTS (SELECT 1 FROM blocks WHERE mh = ?)",
	stmtGet:       "SELECT bytes FROM blocks WHERE mh = ?",
	stmtGetSize:   "SELECT LENGTH(bytes) FROM blocks WHERE mh = ?",
	stmtPut:       "INSERT OR IGNORE INTO blocks (mh, codec, bytes) VALUES (?, ?, ?)",
	stmtDelete:    "DELETE FROM blocks WHERE mh = ?",
	stmtSelectAll: "SELECT mh, codec FROM blocks",
}

// Blockstore is a sqlite backed IPLD blockstore, highly optimized and
// customized for IPLD query and write patterns.
type Blockstore struct {
	db *sql.DB

	prepared [len(statements)]*sql.Stmt
}

var _ blockstore.Blockstore = (*Blockstore)(nil)

type Options struct {
	// placeholder
}

func (b *Blockstore) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	var ret bool
	err := b.prepared[stmtHas].QueryRow(keyFromCid(cid)).Scan(&ret)
	if err != nil {
		err = fmt.Errorf("failed to check for existence of CID %s in sqlite3 blockstore: %w", cid, err)
	}
	return ret, err
}

func (b *Blockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	var data []byte
	switch err := b.prepared[stmtGet].QueryRow(keyFromCid(cid)).Scan(&data); err {
	case sql.ErrNoRows:
		return nil, ipld.ErrNotFound{cid}
	case nil:
		return blocks.NewBlockWithCid(data, cid)
	default:
		return nil, fmt.Errorf("failed to get CID %s from sqlite3 blockstore: %w", cid, err)
	}
}

func (b *Blockstore) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	var size int
	switch err := b.prepared[stmtGetSize].QueryRow(keyFromCid(cid)).Scan(&size); err {
	case sql.ErrNoRows:
		// https://github.com/ipfs/go-ipfs-blockstore/blob/v1.0.1/blockstore.go#L183-L185
		return -1, ipld.ErrNotFound{cid}
	case nil:
		return size, nil
	default:
		return -1, fmt.Errorf("failed to get size of CID %s from sqlite3 blockstore: %w", cid, err)
	}
}

func (b *Blockstore) Put(ctx context.Context, block blocks.Block) error {
	var (
		cid   = block.Cid()
		codec = block.Cid().Prefix().Codec
		data  = block.RawData()
	)

	_, err := b.prepared[stmtPut].Exec(keyFromCid(cid), codec, data)
	if err != nil {
		err = fmt.Errorf("failed to put block with CID %s into sqlite3 blockstore: %w", cid, err)
	}
	return err
}

func (b *Blockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	for i, blk := range blocks {
		if err := b.Put(ctx, blk); err != nil {
			return fmt.Errorf("failed to put block %d/%d with CID %s into sqlite3 blockstore: %w", i, len(blocks), blk.Cid(), err)
		}
	}
	return nil
}

func (b *Blockstore) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	_, err := b.prepared[stmtDelete].Exec(keyFromCid(cid))
	return err
}

func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	ret := make(chan cid.Cid)

	q, err := b.prepared[stmtSelectAll].QueryContext(ctx)
	if err == sql.ErrNoRows {
		close(ret)
		return ret, nil
	} else if err != nil {
		close(ret)
		return nil, fmt.Errorf("failed to query all keys from sqlite3 blockstore: %w", err)
	}

	go func() {
		defer close(ret)

		for ctx.Err() == nil && q.Next() {
			var mhStr string
			var codec uint64

			if err := q.Scan(&mhStr, &codec); err != nil {
				if ctx.Err() != nil {
					// We failed because the context was canceled.
					return
				}
				log.Printf("failed when querying all keys in sqlite3 blockstore: %s", err)
				return
			}

			mh, err := base64.RawStdEncoding.DecodeString(mhStr)
			if err != nil {
				log.Printf("failed to parse multihash when querying all keys in sqlite3 blockstore: %s", err)
				continue
			}

			select {
			case ret <- cid.NewCidV1(codec, mh):
			case <-ctx.Done():
				return
			}
		}
	}()
	return ret, nil
}

func (b *Blockstore) HashOnRead(_ bool) {
	log.Print("sqlite3 blockstore ignored HashOnRead request")
}

func (b *Blockstore) Close() error {
	return b.db.Close()
}

func keyFromCid(c cid.Cid) string {
	return base64.RawStdEncoding.EncodeToString(c.Hash())
}
