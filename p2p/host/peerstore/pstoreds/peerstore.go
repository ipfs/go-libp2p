package pstoreds

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	pstore "github.com/libp2p/go-libp2p/p2p/host/peerstore"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/multiformats/go-base32"
)

// Configuration object for the peerstore.
type Options struct {
	// The size of the in-memory cache. A value of 0 or lower disables the cache.
	CacheSize uint

	// MaxProtocols is the maximum number of protocols we store for one peer.
	MaxProtocols int

	// MaxAddrsPerPeer bounds the number of unconnected addresses stored per
	// peer. When the limit is reached, the unconnected entry with the nearest
	// expiry is evicted before the new entry is inserted. Addresses held by a
	// live connection (TTL >= ConnectedAddrTTL) are not counted and never
	// evicted. A value <= 0 disables the cap.
	MaxAddrsPerPeer int

	// Sweep interval to purge expired addresses from the datastore. If this is a zero value, GC will not run
	// automatically, but it'll be available on demand via explicit calls.
	GCPurgeInterval time.Duration

	// Interval to renew the GC lookahead window. If this is a zero value, lookahead will be disabled and we'll
	// traverse the entire datastore for every purge cycle.
	GCLookaheadInterval time.Duration

	// Initial delay before GC processes start. Intended to give the system breathing room to fully boot
	// before starting GC.
	GCInitialDelay time.Duration

	Clock clock
}

// DefaultOpts returns the default options for a persistent peerstore, with the full-purge GC algorithm:
//
// * Cache size: 1024.
// * MaxProtocols: 1024.
// * MaxAddrsPerPeer: 64.
// * GC purge interval: 2 hours.
// * GC lookahead interval: disabled.
// * GC initial delay: 60 seconds.
func DefaultOpts() Options {
	return Options{
		CacheSize:           1024,
		MaxProtocols:        1024,
		MaxAddrsPerPeer:     64,
		GCPurgeInterval:     2 * time.Hour,
		GCLookaheadInterval: 0,
		GCInitialDelay:      60 * time.Second,
		Clock:               realclock{},
	}
}

type pstoreds struct {
	peerstore.Metrics

	*dsKeyBook
	*dsAddrBook
	*dsProtoBook
	*dsPeerMetadata
}

var _ peerstore.Peerstore = &pstoreds{}

// NewPeerstore creates a peerstore backed by the provided persistent datastore.
// It's the caller's responsibility to call RemovePeer to ensure
// that memory consumption of the peerstore doesn't grow unboundedly.
func NewPeerstore(ctx context.Context, store ds.Batching, opts Options) (*pstoreds, error) {
	addrBook, err := NewAddrBook(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	keyBook, err := NewKeyBook(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	peerMetadata, err := NewPeerMetadata(ctx, store, opts)
	if err != nil {
		return nil, err
	}

	protoBook, err := NewProtoBook(peerMetadata, WithMaxProtocols(opts.MaxProtocols))
	if err != nil {
		return nil, err
	}

	return &pstoreds{
		Metrics:        pstore.NewMetrics(),
		dsKeyBook:      keyBook,
		dsAddrBook:     addrBook,
		dsPeerMetadata: peerMetadata,
		dsProtoBook:    protoBook,
	}, nil
}

// uniquePeerIds extracts and returns unique peer IDs from database keys.
func uniquePeerIds(ds ds.Datastore, prefix ds.Key, extractor func(result query.Result) string) (peer.IDSlice, error) {
	var (
		q       = query.Query{Prefix: prefix.String(), KeysOnly: true}
		results query.Results
		err     error
	)

	if results, err = ds.Query(context.TODO(), q); err != nil {
		log.Error("failed to query database", "err", err)
		return nil, err
	}

	defer results.Close()

	idset := make(map[string]struct{})
	for result := range results.Next() {
		k := extractor(result)
		idset[k] = struct{}{}
	}

	if len(idset) == 0 {
		return peer.IDSlice{}, nil
	}

	ids := make(peer.IDSlice, 0, len(idset))
	for id := range idset {
		pid, err := base32.RawStdEncoding.DecodeString(id)
		if err != nil {
			log.Debugf("failed to decode peer id %s from base32: %v; skipping", id, err)
		        continue
		}
		peerID, err := peer.IDFromBytes(pid)
		if err != nil {
			log.Debugf("failed to create peer id from bytes for %s: %v; skipping", id, err)
			continue
		}
		ids = append(ids, peerID)
	}
	return ids, nil
}

func (ps *pstoreds) Close() (err error) {
	var errs []error
	weakClose := func(name string, c any) {
		if cl, ok := c.(io.Closer); ok {
			if err = cl.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%s error: %s", name, err))
			}
		}
	}
	weakClose("keybook", ps.dsKeyBook)
	weakClose("addressbook", ps.dsAddrBook)
	weakClose("protobook", ps.dsProtoBook)
	weakClose("peermetadata", ps.dsPeerMetadata)

	if len(errs) > 0 {
		return fmt.Errorf("failed while closing peerstore; err(s): %q", errs)
	}
	return nil
}

func (ps *pstoreds) Peers() peer.IDSlice {
	set := map[peer.ID]struct{}{}
	for _, p := range ps.PeersWithKeys() {
		set[p] = struct{}{}
	}
	for _, p := range ps.PeersWithAddrs() {
		set[p] = struct{}{}
	}

	pps := make(peer.IDSlice, 0, len(set))
	for p := range set {
		pps = append(pps, p)
	}
	return pps
}

func (ps *pstoreds) PeerInfo(p peer.ID) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    p,
		Addrs: ps.dsAddrBook.Addrs(p),
	}
}

// RemovePeer removes entries associated with a peer from:
// * the KeyBook
// * the ProtoBook
// * the PeerMetadata
// * the Metrics
// It DOES NOT remove the peer from the AddrBook.
func (ps *pstoreds) RemovePeer(p peer.ID) {
	ps.dsKeyBook.RemovePeer(p)
	ps.dsProtoBook.RemovePeer(p)
	ps.dsPeerMetadata.RemovePeer(p)
	ps.Metrics.RemovePeer(p)
}
