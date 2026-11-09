package client

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConnStatLimited(t *testing.T) {
	c := &Conn{}     //this create  a dummy relay connection
	stat := c.Stat() // this call updated function
	//assert that limited flag is now true
	require.True(t, stat.Limited, "expected relay connection to be marked as Limited")
}
