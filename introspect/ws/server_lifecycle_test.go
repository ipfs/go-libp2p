package ws

import (
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServerClose(t *testing.T) {
	server, _, _ := createTestServer(t)
	err := server.Start()
	require.NoError(t, err)

	// create 100 connections.
	var wg sync.WaitGroup
	conns := make([]*connWrapper, 100)
	wg.Add(100)
	for i := range conns {
		conn := createConn(t, server)
		go func() {
			_, _, err := conn.ReadMessage()
			if strings.Contains(err.Error(), io.ErrUnexpectedEOF.Error()) {
				wg.Done()
			}
		}()
		conns[i] = conn
	}

	err = server.Close()
	require.NoError(t, err)
	wg.Wait()
}

func TestServerHandlesClosedConns(t *testing.T) {
	server, _, _ := createTestServer(t)
	defer server.Close()

	require.NoError(t, server.Start())

	conns := make([]*connWrapper, 50)
	for i := range conns {
		conn := createConn(t, server)
		conns[i] = conn
	}

	require.Eventually(t, func() bool { return len(server.Sessions()) == 50 }, 2*time.Second, 100*time.Millisecond)

	err := conns[0].Close()
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(server.Sessions()) == 49 }, 2*time.Second, 100*time.Millisecond)

	for _, c := range conns[1:] {
		_ = c.Close()
	}
}
