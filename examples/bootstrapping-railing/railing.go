package main

import (
	"context"
	"fmt"

	libp2p "github.com/libp2p/go-libp2p"
	ipfsaddr "github.com/ipfs/go-ipfs-addr"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
)

// The bootstrapping nodes are just long running nodes with a static IP address.
// That means you can easily have your own bootstrapping nodes. Everything you need is
// a server with a static IP address.
var bootstrapPeers = []string{
	"/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
	"/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
	"/ip4/104.236.176.52/tcp/4001/ipfs/QmSoLnSGccFuZQJzRadHn95W2CrSFmZuTdDWP8HXaHca9z",
	"/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
	"/ip4/162.243.248.213/tcp/4001/ipfs/QmSoLueR4xBeUbY9WZ9xGUUxunbKWcrNFTDAadQJmocnWm",
	"/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
	"/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
	"/ip4/178.62.61.185/tcp/4001/ipfs/QmSoLMeWqB7YGVLJN3pNLQpmmEk35v6wYtsMGLzSr5QBU3",
	"/ip4/104.236.151.122/tcp/4001/ipfs/QmSoLju6m7xTh3DuokvT3886QRYqxAzb1kShaanJgW36yx",
}

// This example show's you how you can connect to a list of bootstrapping nodes.
func main() {

	ctx := context.Background()

	// Create the host
	host, err := libp2p.New(ctx, libp2p.Defaults)
	if err != nil {
		panic(err)
	}

	c := make(chan struct{})

	// Loop through the bootstrapping peer list and connect to them
	for _, addr := range bootstrapPeers {

		// Parse the string to an address
		iAddr, err := ipfsaddr.ParseString(addr)
		if err != nil {
			panic(err)
		}

		// Get peer info from multiaddress
		pInfo, err := peerstore.InfoFromP2pAddr(iAddr.Multiaddr())
		if err != nil {
			panic(err)
		}

		go func() {
			// Connect to the peer by it's peer info
			if err := host.Connect(ctx, *pInfo); err != nil {
				fmt.Println("failed to connect to peer: ", err)
				return
			}

			fmt.Println("connected to peer: ", pInfo.ID.String())

			c <- struct{}{}
		}()

	}

	for i := 0; i < 3; i++ {
		<-c
	}

}
