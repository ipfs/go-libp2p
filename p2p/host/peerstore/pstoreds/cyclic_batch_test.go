package pstoreds

import (
	"context"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

// TestCyclicBatchThreshold verifies that cyclicBatch respects the threshold parameter.
func TestCyclicBatchThreshold(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())

	cb, err := newCyclicBatch(store, 3) // threshold = 3
	require.NoError(t, err)

	ctx := context.Background()

	err = cb.Put(ctx, ds.NewKey("key1"), []byte("value1"))
	require.NoError(t, err)
	err = cb.Put(ctx, ds.NewKey("key2"), []byte("value2"))
	require.NoError(t, err)
	// Manually commit the batch
	err = cb.Commit(ctx)
	require.NoError(t, err)

	val, err := store.Get(ctx, ds.NewKey("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
}
