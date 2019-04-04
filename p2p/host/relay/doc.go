/*
The relay package contains host implementations that automatically
advertise relay addresses when the presence of NAT is detected. This
feature is dubbed `autorelay`.

Warning: the internal interfaces are unstable.

System Components:
  - AutoNATService instances -- see https://github.com/libp2p/go-libp2p-autonat-svc
  - One or more relays, instances of `RelayHost`
  - The autorelayed hosts, instances of `AutoRelayHost`.

How it works:

- AutoNATService
  `AutoNATService` instances are instantiated in the
  bootstrappers (or other well known publicly reachable hosts)

- RelayHost
  RelayHosts provide Relay Hop services, and advertise through the DHT
  in the `/libp2p/relay` namespace

- AutoRelayHost
  AutoRelayHosts passively discover autonat service instances and test dialability of
  their listen address set through them.  When the presence of NAT is detected,
  they discover relays through the DHT, connect to some of them and begin
  advertising relay addresses.  The new set of addresses is propagated to
  connected peers through the `identify/push` protocol.

*/
package relay
