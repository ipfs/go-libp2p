package pstoreds

import (
	"context"
	"errors"
	"fmt"

	ds "github.com/ipfs/go-datastore"
)

// how many operations are queued in a cyclic batch before we flush it.
var defaultOpsPerCyclicBatch = 20

// cyclicBatch buffers ds write operations and automatically flushes them after defaultOpsPerCyclicBatch (20) have been
// queued. An explicit `Commit()` closes this cyclic batch, erroring all further operations.
//
// It is similar to go-ds autobatch, but it's driven by an actual Batch facility offered by the
// ds.
type cyclicBatch struct {
	threshold int
	ds.Batch
	ds      ds.Batching
	pending int
}

func newCyclicBatch(ds ds.Batching, _ int) (ds.Batch, error) {
	batch, err := ds.Batch(context.TODO())
	if err != nil {
		return nil, err
	}
	return &cyclicBatch{Batch: batch, ds: ds, threshold: threshold}, nil
}

func (cb *cyclicBatch) cycle() (err error) {
	if cb.Batch == nil {
		return errors.New("cyclic batch is closed")
	}
	if cb.pending < cb.threshold {
		// we haven't reached the threshold yet.
		return nil
	}
	// commit and renew the batch.
	if err = cb.Batch.Commit(context.TODO()); err != nil {
		return fmt.Errorf("failed while committing cyclic batch: %w", err)
	}
	if cb.Batch, err = cb.ds.Batch(context.TODO()); err != nil {
		return fmt.Errorf("failed while renewing cyclic batch: %w", err)
	}
	return nil
}

func (cb *cyclicBatch) Put(ctx context.Context, key ds.Key, val []byte) error {
	if err := cb.cycle(); err != nil {
		return err
	}
	cb.pending++
	return cb.Batch.Put(ctx, key, val)
}

func (cb *cyclicBatch) Delete(ctx context.Context, key ds.Key) error {
	if err := cb.cycle(); err != nil {
		return err
	}
	cb.pending++
	return cb.Batch.Delete(ctx, key)
}

func (cb *cyclicBatch) Commit(ctx context.Context) error {
	if cb.Batch == nil {
		return errors.New("cyclic batch is closed")
	}
	if err := cb.Batch.Commit(ctx); err != nil {
		return err
	}
	cb.pending = 0
	cb.Batch = nil
	return nil
}
