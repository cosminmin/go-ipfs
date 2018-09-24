package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	p2pdiscovery "gx/ipfs/QmPL3AKtiaQyYpchZceXBZhZ3MSnoGqJvLZrc7fzDTTQdJ/go-libp2p/p2p/discovery"
	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	routing "gx/ipfs/QmPmFeQ5oY5G6M7aBWggi5phxEPXwsQntE1DFcUzETULdp/go-libp2p-routing"
	p2pcrypto "gx/ipfs/QmPvyPwuCgJ7pDmrKDxRtsScJgBaM5h4EpRL2qQJsmXf4n/go-libp2p-crypto"
	blocks "gx/ipfs/QmRcHuYzAyswytBuMF78rj3LTChYszomRFXNg4685ZN1WM/go-block-format"
	p2pnet "gx/ipfs/QmSTaEYUgDe1r581hxyd2u9582Hgp3KX4wGwYbRqz2u9Qh/go-libp2p-net"
	record "gx/ipfs/QmSb4B8ZAAj5ALe9LjfzPyF8Ma6ezC1NTnDF2JQPUJxEXb/go-libp2p-record"
	dht "gx/ipfs/QmSteomMgXnSQxLEY5UpxmkYAd8QF9JuLLeLYBokTHxFru/go-libp2p-kad-dht"
	dhtopts "gx/ipfs/QmSteomMgXnSQxLEY5UpxmkYAd8QF9JuLLeLYBokTHxFru/go-libp2p-kad-dht/opts"
	floodsub "gx/ipfs/QmTcC9Qx2adsdGguNpqZ6dJK7MMsH8sf3yfxZxG3bSwKet/go-libp2p-floodsub"
	dns "gx/ipfs/QmWchsfMt9Re1CQaiHqPQC1DrZ9bkpa6n229dRYkGyLXNh/dns"
	peerstore "gx/ipfs/QmWtCpWB39Rzc2xTB75MKorsxNpo3TyecTEN24CJ3KVohE/go-libp2p-peerstore"
	ipns "gx/ipfs/QmX72XT6sSQRkNHKcAFLM2VqB3B4bWPetgWnHY8LgsUVeT/go-ipns"
	ipnspb "gx/ipfs/QmX72XT6sSQRkNHKcAFLM2VqB3B4bWPetgWnHY8LgsUVeT/go-ipns/pb"
	maddr "gx/ipfs/QmYmsdtJ3HsodkePE3eU3TsCaP2YvPZJ4LoXnNkDE5Tpt7/go-multiaddr"
	datastore "gx/ipfs/QmaRb5yNXKonhbkpNxNawoydk4N6es6b4fPj19sjEKsh5D/go-datastore"
	p2ppeer "gx/ipfs/QmbNepETomvmXfz1X5pHNFD2QuPqnqi47dTd94QJWSorQ3/go-libp2p-peer"
	proto "gx/ipfs/QmdxUuburamoF6zF9qjeQC4WYcWGbWuRmdLacMEsW8ioD8/gogo-protobuf/proto"
	multibase "gx/ipfs/QmekxXDhCxCJRNuzmHreuaT3BsuJcsjcXWNrtV9C8DRHtd/go-multibase"
	p2phost "gx/ipfs/Qmf5yHzmWAyHSJRPAmZzfk3Yd7icydBLi7eec5741aov7v/go-libp2p-host"
)

type Daemon struct {
	Context   context.Context
	Host      p2phost.Host
	Routing   routing.IpfsRouting
	Discovery p2pdiscovery.Service
	PubSub    *floodsub.PubSub
	Updates   *floodsub.Subscription

	entries      map[p2ppeer.ID]*ipnspb.IpnsEntry
	entriesMutex sync.RWMutex
}

func NewDaemon(ctx context.Context, host p2phost.Host) (*Daemon, error) {
	d := &Daemon{Context: ctx, Host: host}

	pubsub, err := floodsub.NewGossipSub(ctx, host)
	if err != nil {
		return nil, err
	}

	interval := 5 * time.Second
	discovery, err := p2pdiscovery.NewMdnsService(ctx, host, interval, p2pdiscovery.ServiceTag)
	if err != nil {
		return nil, err
	}
	discovery.RegisterNotifee(d)

	ds := datastore.NewMapDatastore()
	validator := record.NamespacedValidator{
		// "pk":   record.PublicKeyValidator{},
		"ipns": ipns.Validator{KeyBook: host.Peerstore()},
	}
	dht, err := dht.New(ctx, host, dhtopts.Datastore(ds), dhtopts.Validator(validator))
	if err != nil {
		return nil, err
	}

	d.PubSub = pubsub
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

	cid := blocks.NewBlock([]byte("floodsub:" + topic)).Cid()

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

	cid := blocks.NewBlock([]byte("floodsub:" + topic)).Cid()

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

func (d *Daemon) validateMessage(ctx context.Context, msg *floodsub.Message) bool {
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
