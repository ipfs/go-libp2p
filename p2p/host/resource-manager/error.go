package rcmgr

type ErrStreamOrConnLimitExceeded struct {
	current, attempted, limit int
	err                       error
}

func (e *ErrStreamOrConnLimitExceeded) Error() string { return e.err.Error() }
func (e *ErrStreamOrConnLimitExceeded) Unwrap() error { return e.err }

type ErrMemoryLimitExceeded struct {
	current, attempted, limit int64
	priority                  uint8
	err                       error
}

func (e *ErrMemoryLimitExceeded) Error() string { return e.err.Error() }
func (e *ErrMemoryLimitExceeded) Unwrap() error { return e.err }
