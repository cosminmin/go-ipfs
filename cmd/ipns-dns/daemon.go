package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	maddr "gx/ipfs/QmNTCey11oxhb1AxDnQBRHtdhap6Ctud872NjAYPYYXPuc/go-multiaddr"
	p2pnet "gx/ipfs/QmNgLg1NTw37iWbYPKcyK85YJ9Whs1MkPtJwhfqbNYAyKg/go-libp2p-net"
	p2pcrypto "gx/ipfs/QmNiJiXwWE3kRhZrC5ej3kSjWHm337pYfhjLGSCDNKJP2s/go-libp2p-crypto"
	dht "gx/ipfs/QmNoNExMdWrYSPZDiJJTVmxSh6uKLN26xYVzbLzBLedRcv/go-libp2p-kad-dht"
	dhtopts "gx/ipfs/QmNoNExMdWrYSPZDiJJTVmxSh6uKLN26xYVzbLzBLedRcv/go-libp2p-kad-dht/opts"
	peerstore "gx/ipfs/QmPiemjiKBC9VA7vZF82m4x1oygtg2c2YVqag8PX7dN1BD/go-libp2p-peerstore"
	cid "gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	routing "gx/ipfs/QmTiRqrF5zkdZyrdsL5qndG1UbeWi8k8N2pYxCtXWrahR2/go-libp2p-routing"
	pubsub "gx/ipfs/QmVRxA4J3UPQpw74dLrQ6NJkfysCA1H4GU28gVpXQt9zMU/go-libp2p-pubsub"
	ipns "gx/ipfs/QmWPFehHmySCdaGttQ48iwF7M6mBRrGE5GSPWKCuMWqJDR/go-ipns"
	ipnspb "gx/ipfs/QmWPFehHmySCdaGttQ48iwF7M6mBRrGE5GSPWKCuMWqJDR/go-ipns/pb"
	dns "gx/ipfs/QmWchsfMt9Re1CQaiHqPQC1DrZ9bkpa6n229dRYkGyLXNh/dns"
	blocks "gx/ipfs/QmWoXtvgC8inqFkAATB7cp2Dax7XBi9VDvSg9RCCZufmRk/go-block-format"
	p2ppeer "gx/ipfs/QmY5Grm8pJdiSSVsYxx4uNRgweY72EmYwuSDbRnbFok3iY/go-libp2p-peer"
	p2pdiscovery "gx/ipfs/QmYxivS34F2M2n44WQQnRHGAKS8aoRUxwGpi9wk4Cdn4Jf/go-libp2p/p2p/discovery"
	p2phost "gx/ipfs/QmaoXrM4Z41PD48JY36YqQGKQpLGjyLA2cKcLsES7YddAq/go-libp2p-host"
	proto "gx/ipfs/QmdxUuburamoF6zF9qjeQC4WYcWGbWuRmdLacMEsW8ioD8/gogo-protobuf/proto"
	multibase "gx/ipfs/QmekxXDhCxCJRNuzmHreuaT3BsuJcsjcXWNrtV9C8DRHtd/go-multibase"
	datastore "gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore"
	record "gx/ipfs/QmfARXVCzpwFXQdepAJZuqyNDgV9doEsMnVCo1ssmuSe1U/go-libp2p-record"
)

type Daemon struct {
	Context   context.Context
	Host      p2phost.Host
	Routing   routing.IpfsRouting
	Discovery p2pdiscovery.Service
	PubSub    *pubsub.PubSub
	Updates   *pubsub.Subscription

	entries      map[p2ppeer.ID]*ipnspb.IpnsEntry
	entriesMutex sync.RWMutex
}

func NewDaemon(ctx context.Context, host p2phost.Host) (*Daemon, error) {
	d := &Daemon{Context: ctx, Host: host}

	psub, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("gossipsub: %s", err)
	}

	interval := 5 * time.Second
	discovery, err := p2pdiscovery.NewMdnsService(ctx, host, interval, p2pdiscovery.ServiceTag)
	if err != nil {
		return nil, fmt.Errorf("discovery: %s", err)
	}
	discovery.RegisterNotifee(d)

	ds := datastore.NewMapDatastore()
	validator := record.NamespacedValidator{
		// "pk":   record.PublicKeyValidator{},
		"ipns": ipns.Validator{KeyBook: host.Peerstore()},
	}
	dht, err := dht.New(ctx, host, dhtopts.Datastore(ds), dhtopts.Validator(validator))
	if err != nil {
		return nil, fmt.Errorf("dht: %s", err)
	}

	d.PubSub = psub
	d.Discovery = discovery
	d.Routing = dht

	d.entries = map[p2ppeer.ID]*ipnspb.IpnsEntry{}

	return d, nil
}

func (d *Daemon) Bootstrap(ctx context.Context, addrs []string, topic string) error {
	if err := d.BootstrapNetwork(ctx, addrs); err != nil {
		return err
	}
	return d.Subscribe(topic)
}

func (d *Daemon) BootstrapNetwork(ctx context.Context, addrs []string) error {
	for _, a := range addrs {
		pinfo, err := peerstore.InfoFromP2pAddr(maddr.StringCast(a))
		if err != nil {
			return err
		}
		if err = d.Host.Connect(ctx, *pinfo); err != nil {
			return err
		}
		fmt.Printf("connected: /p2p/%s\n", pinfo.ID.Pretty())
	}

	return nil
}

func (d *Daemon) HandlePeerFound(pinfo peerstore.PeerInfo) {
	connectTimeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(d.Context, connectTimeout)
	defer cancel()

	connected := d.Host.Network().Connectedness(pinfo.ID) == p2pnet.Connected
	if connected {
		return
	}

	fmt.Printf("found: /p2p/%s %+v\n", pinfo.ID.Pretty(), pinfo.Addrs)

	if err := d.Host.Connect(ctx, pinfo); err == nil {
		fmt.Printf("connected: /p2p/%s\n", pinfo.ID.Pretty())
	}
}

func (d *Daemon) AnnouncePubsub(ctx context.Context, topic string) error {
	timeout := 120 * time.Second

	cid := blocks.NewBlock([]byte("pubsub:" + topic)).Cid()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := d.Routing.Provide(ctx, cid, true); err != nil {
		return err
	}

	return nil
}

func (d *Daemon) MaintainPubsub(ctx context.Context, topic string) error {
	searchMax := 10
	searchTimeout := 30 * time.Second
	connectTimeout := 10 * time.Second

	cid := blocks.NewBlock([]byte("pubsub:" + topic)).Cid()

	sctx, cancel := context.WithTimeout(ctx, searchTimeout)
	defer cancel()

	provs := d.Routing.FindProvidersAsync(sctx, cid, searchMax)
	wg := &sync.WaitGroup{}
	for p := range provs {
		wg.Add(1)
		go func(pi peerstore.PeerInfo) {
			defer wg.Done()
			ctx, cancel2 := context.WithTimeout(ctx, connectTimeout)
			defer cancel2()
			err := d.Host.Connect(ctx, pi)
			if err != nil {
				return
			}
		}(p)
	}
	wg.Wait()

	return nil
}

func (d *Daemon) Subscribe(topic string) error {
	if err := d.PubSub.RegisterTopicValidator(topic, d.validateMessage); err != nil {
		return err
	}

	sub, err := d.PubSub.Subscribe(topic)
	if err != nil {
		return err
	}

	d.Updates = sub
	return nil
}

func (d *Daemon) validateMessage(ctx context.Context, msg *pubsub.Message) bool {
	return true
}

func (d *Daemon) ReceiveUpdates(ctx context.Context) {
	validator := ipns.Validator{}
	for {
		msg, err := d.Updates.Next(ctx)
		if err != nil {
			// fmt.Printf("update: updates.next: %s\n", err)
			continue
		}

		entry := new(ipnspb.IpnsEntry)
		err = proto.Unmarshal(msg.Data, entry)
		if err != nil {
			// fmt.Printf("update: unmarshal: %s\n", err)
			continue
		}

		pubkey, err := p2pcrypto.UnmarshalPublicKey(entry.GetPubKey())
		if err != nil {
			// fmt.Printf("update: pubkey: %s\n", err)
			continue
		}

		peerid, err := p2ppeer.IDFromPublicKey(pubkey)
		if err != nil {
			// fmt.Printf("update: peerid: %s\n", err)
			continue
		}

		err = validator.Validate("/ipns/"+string(peerid), msg.Data)
		if err != nil {
			// fmt.Printf("update: validate: %s\n", err)
			continue
		}

		// store entry
		d.entriesMutex.Lock()
		d.entries[peerid] = entry
		d.entriesMutex.Unlock()

		fmt.Printf("update: /ipns/%s => %s\n", peerid.Pretty(), entry.GetValue())
	}
}

func (d *Daemon) GetEntry(peerid p2ppeer.ID) (*ipnspb.IpnsEntry, bool) {
	d.entriesMutex.RLock()
	defer d.entriesMutex.RUnlock()

	entry, ok := d.entries[peerid]
	return entry, ok
}

func (d *Daemon) StartDNS(ctx context.Context, address, network string) {
	handler := &dnsServer{getEntry: d.GetEntry, network: network}
	err := dns.ListenAndServe(address, network, handler)
	if err != nil {
		fmt.Printf("dns server: %s\n", err)
	}
}

type dnsServer struct {
	getEntry func(p2ppeer.ID) (*ipnspb.IpnsEntry, bool)
	network  string
}

func (dnsserv *dnsServer) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	fmt.Printf("dns(%s): request: %+v\n", dnsserv.network, r.Question)

	if len(r.Question) == 0 {
		// fmt.Printf("dns(%s): no question asked\n", dnsserv.network)
		return
	}

	q := r.Question[0]

	if q.Qtype != dns.TypeTXT {
		// fmt.Printf("dns(%s): i only speak TXT\n", dnsserv.network)
		return
	}

	labels := dns.SplitDomainName(q.Name)
	peercid, err := cid.Decode(labels[0])
	if err != nil {
		// fmt.Printf("dns(%s): cid error: %s\n", dnsserv.network, err)
		return
	}

	peerid, err := p2ppeer.IDFromBytes(peercid.Hash())
	if err != nil {
		// fmt.Printf("dns(%s): peerid error: %s\n", dnsserv.network, err)
		return
	}

	hdr := dns.RR_Header{Ttl: 1, Class: dns.ClassINET, Rrtype: dns.TypeTXT}
	hdr.Name = strings.Join(labels, ".") + "."

	entry, ok := dnsserv.getEntry(peerid)
	if !ok {
		m := new(dns.Msg)
		m.SetRcode(r, dns.RcodeSuccess)
		m.Answer = []dns.RR{&dns.TXT{Hdr: hdr, Txt: []string{"ipns="}}}
		m.Authoritative = true
		w.WriteMsg(m)
		fmt.Printf("dns(%s): nxdomain: /ipns/%s\n", dnsserv.network, peerid.Pretty())
		return
	}

	m := new(dns.Msg)
	m.SetReply(r)
	if e := m.IsEdns0(); e != nil {
		// fmt.Printf("dns(%s): edns0: 4096 bytes\n", dnsserv.network)
		m.SetEdns0(4096, e.Do())
	} else if dnsserv.network == "udp" {
		m.Truncated = true
	}
	m.Authoritative = true

	data, err := proto.Marshal(entry)
	if err != nil {
		// fmt.Printf("dns(%s): protobuf error: %s\n", dnsserv.network, err)
		return
	}

	bigtxt := "ipns=" + multibase.MustNewEncoder(multibase.Base32).Encode(data)
	biglen := len(bigtxt)
	txt := []string{}
	for biglen > 0 {
		pos := 254
		if biglen < 254 {
			pos = biglen
		}
		txt = append(txt, bigtxt[:pos])
		bigtxt = bigtxt[pos:]
		biglen = len(bigtxt)
	}

	m.Answer = []dns.RR{&dns.TXT{Hdr: hdr, Txt: txt}}
	m.SetRcode(r, dns.RcodeSuccess)
	w.WriteMsg(m)

	fmt.Printf("dns(%s): ok: /ipns/%s\n", dnsserv.network, peerid.Pretty())
}
