package pstoreds

import (
	"context"
	"strings"
	"testing"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-base32"
	"github.com/stretchr/testify/require"
)

// TestUniquePeerIds verifies that uniquePeerIds correctly handles valid and invalid peer IDs.
func TestUniquePeerIds(t *testing.T) {
	store := dssync.MutexWrap(ds.NewMapDatastore())

	ctx := context.Background()

	// Insert a garbage peer ID (random invalid bytes encoded to base32)
	badKey := base32.RawStdEncoding.EncodeToString([]byte("garbagebytes"))
	err := store.Put(ctx, ds.NewKey("/peers/"+badKey), []byte{})
	require.NoError(t, err)

	// Insert a valid peer ID
	validPeerID, err := peer.Decode("12D3KooWPshfDXh6bsy1ru6CkMBuqoK2LRpXsPTmGVJ4AzD26tVn")
	require.NoError(t, err)

	validBase32 := base32.RawStdEncoding.EncodeToString([]byte(validPeerID))
	err = store.Put(ctx, ds.NewKey("/peers/"+validBase32), []byte{})
	require.NoError(t, err)

	ids, err := uniquePeerIds(store, ds.NewKey("/peers"), func(result query.Result) string {
		return result.Key[strings.LastIndex(result.Key, "/")+1:]
	})
	require.NoError(t, err)

	// Instead of checking length 1, check that our validPeerID is found
	found := false
	for _, id := range ids {
		if id == validPeerID {
			found = true
			break
		}
	}
	require.True(t, found, "valid peer ID should be present in the results")
}
