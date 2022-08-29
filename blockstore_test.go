//go:build cgo

package sqlite3bs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
	u "github.com/ipfs/go-ipfs-util"

	"github.com/stretchr/testify/require"
)

func TestGetWhenKeyNotPresent(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	c := cid.NewCidV0(u.Hash([]byte("stuff")))
	bl, err := bs.Get(c)
	require.Nil(t, bl)
	require.Equal(t, blockstore.ErrNotFound, err)
}

func TestGetWhenKeyIsNil(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	_, err := bs.Get(cid.Undef)
	require.Equal(t, blockstore.ErrNotFound, err)
}

func TestPutThenGetBlock(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(orig)
	require.NoError(t, err)

	fetched, err := bs.Get(orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestHas(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(orig)
	require.NoError(t, err)

	ok, err := bs.Has(orig.Cid())
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = bs.Has(blocks.NewBlock([]byte("another thing")).Cid())
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCidv0v1(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(orig)
	require.NoError(t, err)

	fetched, err := bs.Get(cid.NewCidV1(cid.DagProtobuf, orig.Cid().Hash()))
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestPutThenGetSizeBlock(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	block := blocks.NewBlock([]byte("some data"))
	missingBlock := blocks.NewBlock([]byte("missingBlock"))
	emptyBlock := blocks.NewBlock([]byte{})

	err := bs.Put(block)
	require.NoError(t, err)

	blockSize, err := bs.GetSize(block.Cid())
	require.NoError(t, err)
	require.Len(t, block.RawData(), blockSize)

	err = bs.Put(emptyBlock)
	require.NoError(t, err)

	emptySize, err := bs.GetSize(emptyBlock.Cid())
	require.NoError(t, err)
	require.Zero(t, emptySize)

	missingSize, err := bs.GetSize(missingBlock.Cid())
	require.Equal(t, blockstore.ErrNotFound, err)
	require.Equal(t, -1, missingSize)
}

func TestAllKeysSimple(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	keys := insertBlocks(t, bs, 100)

	ctx := context.Background()
	ch, err := bs.AllKeysChan(ctx)
	require.NoError(t, err)
	actual := collect(ch)

	require.ElementsMatch(t, keys, actual)
}

func TestAllKeysRespectsContext(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	_ = insertBlocks(t, bs, 100)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := bs.AllKeysChan(ctx)
	require.NoError(t, err)

	// consume 2, then cancel context.
	v, ok := <-ch
	require.NotEqual(t, cid.Undef, v)
	require.True(t, ok)

	v, ok = <-ch
	require.NotEqual(t, cid.Undef, v)
	require.True(t, ok)

	cancel()

	received := 0
	for range ch {
		received++
		require.LessOrEqual(t, received, 10, "expected query to be canceled")
	}
}

func TestDoubleClose(t *testing.T) {
	bs, _ := newBlockstore(t)
	require.NoError(t, bs.Close())
	require.NoError(t, bs.Close())
}

func TestReopenPutGet(t *testing.T) {
	bs, path := newBlockstore(t)

	orig := blocks.NewBlock([]byte("some data"))
	err := bs.Put(orig)
	require.NoError(t, err)

	err = bs.Close()
	require.NoError(t, err)

	bs, err = Open(path, Options{})
	require.NoError(t, err)

	fetched, err := bs.Get(orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestPutMany(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(blks)
	require.NoError(t, err)

	for _, blk := range blks {
		fetched, err := bs.Get(blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), fetched.RawData())

		ok, err := bs.Has(blk.Cid())
		require.NoError(t, err)
		require.True(t, ok)
	}

	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)

	cids := collect(ch)
	require.Len(t, cids, 3)
}

func TestDelete(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(blks)
	require.NoError(t, err)

	err = bs.DeleteBlock(blks[1].Cid())
	require.NoError(t, err)

	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)

	cids := collect(ch)
	require.Len(t, cids, 2)
	require.ElementsMatch(t, cids, []cid.Cid{
		cid.NewCidV1(cid.DagProtobuf, blks[0].Cid().Hash()),
		cid.NewCidV1(cid.DagProtobuf, blks[2].Cid().Hash()),
	})

	has, err := bs.Has(blks[1].Cid())
	require.NoError(t, err)
	require.False(t, has)

}

func newBlockstore(tb testing.TB) (*Blockstore, string) {
	tb.Helper()

	tmp, err := ioutil.TempFile("", "")
	if err != nil {
		tb.Fatal(err)
	}

	path := tmp.Name()
	db, err := Open(path, Options{})
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		_ = os.RemoveAll(path)
	})

	return db, path
}

func insertBlocks(t *testing.T, bs *Blockstore, count int) []cid.Cid {
	keys := make([]cid.Cid, count)
	for i := 0; i < count; i++ {
		block := blocks.NewBlock([]byte(fmt.Sprintf("some data %d", i)))
		err := bs.Put(block)
		require.NoError(t, err)
		// NewBlock assigns a CIDv0; we convert it to CIDv1 because that's what
		// the store returns.
		keys[i] = cid.NewCidV1(cid.DagProtobuf, block.Multihash())
	}
	return keys
}

func collect(ch <-chan cid.Cid) []cid.Cid {
	var keys []cid.Cid
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}
