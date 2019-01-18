package main

// /ipns/QmWjFr2JgC1rirRGMeLYxR3EEjCjK34DaRRHuz2PLVpTgb
// bafkreid4vaai4tak4nf5xqrvsridzlkg5ry26y3tyadk4avsje3z5zljta.ipns.name

import (
	"context"
	"fmt"
	"os"
	"time"

	libp2p "gx/ipfs/QmYxivS34F2M2n44WQQnRHGAKS8aoRUxwGpi9wk4Cdn4Jf/go-libp2p"
	manet "gx/ipfs/QmZcLBXKaFe8ND5YHPkJRAwmhJGrVsi1JqDZNyJ4nRK5Mj/go-multiaddr-net"
)

const topic = "/ipns/.well-known/all"

var bootstrap = []string{
	"/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
	"/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
	"/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
	"/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(os.Args) == 1 {
		fmt.Printf("usage: ipns-dns <dns-listen-addr> (e.g. ipns-dns 127.0.0.1:53)\n")
		os.Exit(1)
	}
	dnsAddress := os.Args[1]

	host, err := libp2p.New(ctx)
	if err != nil {
		panic(err)
	}

	for _, ma := range host.Network().ListenAddresses() {
		fmt.Printf("listening: %s/p2p/%s\n", ma.String(), host.ID().Pretty())
	}

	d, err := NewDaemon(ctx, host)
	if err != nil {
		panic(err)
	}
	if err = d.Bootstrap(ctx, bootstrap, topic); err != nil {
		panic(err)
	}

	fmt.Printf("bootstrapped: ok\n")

	go d.ReceiveUpdates(ctx)
	go d.StartDNS(ctx, dnsAddress, "udp")
	go d.StartDNS(ctx, dnsAddress, "tcp")

	go func() {
		for {
			fmt.Printf("announcing pubsub...\n")
			d.AnnouncePubsub(ctx, topic)
			fmt.Printf("announcing pubsub: done\n")
			time.Sleep(30 * time.Second)
		}
	}()

	go func() {
		for {
			fmt.Printf("maintaining pubsub...\n")
			d.MaintainPubsub(ctx, topic)
			fmt.Printf("maintaining pubsub: done\n")
			time.Sleep(30 * time.Second)
		}
	}()

	go func() {
		for range time.Tick(10 * time.Second) {
			conns := host.Network().Conns()
			local := 0
			for _, conn := range conns {
				if manet.IsIPLoopback(conn.RemoteMultiaddr()) {
					local++
				}
			}

			fmt.Printf("peers: total %d, local %d, topic %d\n",
				len(conns), local, len(d.PubSub.ListPeers(topic)))
		}
	}()

	select {}
}
