package discovery

import (
	"errors"
	"testing"
	"time"
)

func TestTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wantTTL time.Duration
		wantErr error
	}{
		{
			name:    "negative TTL is rejected",
			ttl:     -time.Second,
			wantErr: ErrNegativeTTL,
		},
		{
			name:    "zero TTL means no hint",
			ttl:     0,
			wantTTL: 0,
		},
		{
			name:    "positive TTL is used as is",
			ttl:     time.Minute,
			wantTTL: time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts Options
			err := opts.Apply(TTL(tt.ttl))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Apply returned error %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if opts.Ttl != tt.wantTTL {
				t.Fatalf("Ttl = %v, want %v", opts.Ttl, tt.wantTTL)
			}
		})
	}
}

func TestApplyReportsNegativeTTL(t *testing.T) {
	var opts Options
	err := opts.Apply(Limit(10), TTL(time.Hour), TTL(-time.Second), Limit(20))
	if !errors.Is(err, ErrNegativeTTL) {
		t.Fatalf("Apply returned error %v, want %v", err, ErrNegativeTTL)
	}
	// options applied before the failing one are kept, the ones after it are not
	if opts.Limit != 10 {
		t.Fatalf("Limit = %d, want 10", opts.Limit)
	}
	if opts.Ttl != time.Hour {
		t.Fatalf("Ttl = %v, want %v", opts.Ttl, time.Hour)
	}
}
