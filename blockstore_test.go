//go:build cgo

package sqlite3bs

import (
	"context"
	"fmt"
	"os"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/stretchr/testify/require"
)

func TestGetWhenKeyNotPresent(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	ctx := context.Background()

	c := cid.NewCidV0(u.Hash([]byte("stuff")))
	bl, err := bs.Get(ctx, c)
	require.Nil(t, bl)
	require.True(t, ipld.IsNotFound(err))
}

func TestGetWhenKeyIsNil(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	_, err := bs.Get(context.Background(), cid.Undef)
	require.True(t, ipld.IsNotFound(err))
}

func TestPutThenGetBlock(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	orig := blocks.NewBlock([]byte("some data"))

	ctx := context.Background()
	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	fetched, err := bs.Get(ctx, orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestHas(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	orig := blocks.NewBlock([]byte("some data"))

	ctx := context.Background()
	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	ok, err := bs.Has(ctx, orig.Cid())
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = bs.Has(ctx, blocks.NewBlock([]byte("another thing")).Cid())
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCidv0v1(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	orig := blocks.NewBlock([]byte("some data"))

	ctx := context.Background()
	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	fetched, err := bs.Get(ctx, cid.NewCidV1(cid.DagProtobuf, orig.Cid().Hash()))
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestPutThenGetSizeBlock(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	block := blocks.NewBlock([]byte("some data"))
	missingBlock := blocks.NewBlock([]byte("missingBlock"))
	emptyBlock := blocks.NewBlock([]byte{})

	ctx := context.Background()
	err := bs.Put(ctx, block)
	require.NoError(t, err)

	blockSize, err := bs.GetSize(ctx, block.Cid())
	require.NoError(t, err)
	require.Len(t, block.RawData(), blockSize)

	err = bs.Put(ctx, emptyBlock)
	require.NoError(t, err)

	emptySize, err := bs.GetSize(ctx, emptyBlock.Cid())
	require.NoError(t, err)
	require.Zero(t, emptySize)

	missingSize, err := bs.GetSize(ctx, missingBlock.Cid())
	require.True(t, ipld.IsNotFound(err))
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

	ctx := context.Background()

	orig := blocks.NewBlock([]byte("some data"))
	err := bs.Put(ctx, orig)
	require.NoError(t, err)

	err = bs.Close()
	require.NoError(t, err)

	bs, err = Open(path, Options{})
	require.NoError(t, err)

	fetched, err := bs.Get(ctx, orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestPutMany(t *testing.T) {
	bs, _ := newBlockstore(t)
	defer func() { require.NoError(t, bs.Close()) }()

	ctx := context.Background()

	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(ctx, blks)
	require.NoError(t, err)

	for _, blk := range blks {
		fetched, err := bs.Get(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), fetched.RawData())

		ok, err := bs.Has(ctx, blk.Cid())
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

	ctx := context.Background()
	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(ctx, blks)
	require.NoError(t, err)

	err = bs.DeleteBlock(ctx, blks[1].Cid())
	require.NoError(t, err)

	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)

	cids := collect(ch)
	require.Len(t, cids, 2)
	require.ElementsMatch(t, cids, []cid.Cid{
		cid.NewCidV1(cid.DagProtobuf, blks[0].Cid().Hash()),
		cid.NewCidV1(cid.DagProtobuf, blks[2].Cid().Hash()),
	})

	has, err := bs.Has(ctx, blks[1].Cid())
	require.NoError(t, err)
	require.False(t, has)

}

func newBlockstore(tb testing.TB) (*Blockstore, string) {
	tb.Helper()

	tmp, err := os.CreateTemp("", "")
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
	ctx := context.Background()
	keys := make([]cid.Cid, count)
	for i := 0; i < count; i++ {
		block := blocks.NewBlock([]byte(fmt.Sprintf("some data %d", i)))
		err := bs.Put(ctx, block)
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
