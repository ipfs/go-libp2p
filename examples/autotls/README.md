# libp2p host with Secure WebSockets and AutoTLS

This example builds on the [libp2p host example](../libp2p-host) example and demonstrates how to use [AutoTLS](https://libp2p.io/blog/autotls/) to automatically generate a wildcard Let's Encrypt TLS certificate unique to the libp2p host (`*.<PeerID>.libp2p.direct`), and use it with [libp2p WebSockets transport over TCP](https://github.com/libp2p/specs/blob/master/websockets/README.md) enabling browsers to directly connect to the libp2p host.

For this example to work, you need to have a public IP address and be publicly reachable. AutoTLS is guarded by connectivity check, and will not ask for a certificate unless your libp2p node emits `event.EvtLocalReachabilityChanged` with `network.ReachabilityPublic`.

## Running the example

From the `go-libp2p/examples` directory run the following:

```sh
cd autotls/
go run .
```
