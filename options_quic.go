package libp2p

// This file contains QUIC-specific options.
// By separating these from options.go, we avoid forcing imports of quicreuse
// when users don't need QUIC transport.

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"go.uber.org/fx"
)

// QUICReuse configures libp2p to reuse QUIC connections.
func QUICReuse(constructor any, opts ...quicreuse.Option) Option {
	return func(cfg *Config) error {
		tag := `group:"quicreuseopts"`
		typ := reflect.ValueOf(constructor).Type()
		numParams := typ.NumIn()
		isVariadic := typ.IsVariadic()

		if !isVariadic && len(opts) > 0 {
			return errors.New("QUICReuse constructor doesn't take any options")
		}

		var params []string
		if isVariadic && len(opts) > 0 {
			// If there are options, apply the tag.
			// Since options are variadic, they have to be the last argument of the constructor.
			params = make([]string, numParams)
			params[len(params)-1] = tag
		}

		cfg.QUICReuse = append(cfg.QUICReuse, fx.Provide(fx.Annotate(constructor, fx.ParamTags(params...))))
		for _, opt := range opts {
			cfg.QUICReuse = append(cfg.QUICReuse, fx.Supply(fx.Annotate(opt, fx.ResultTags(tag))))
		}
		return nil
	}
}

// transportOptID generates a random identifier for transport options
func transportOptID() uint64 {
	b := make([]byte, 8)
	rand.Read(b)
	return binary.BigEndian.Uint64(b)
}
