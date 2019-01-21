package dnspubsub

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	namesys "github.com/ipfs/go-ipfs/namesys"
	namesysopt "github.com/ipfs/go-ipfs/namesys/opts"

	path "gx/ipfs/QmNYPETsdAu2uQ1k9q9S1jYEGURaLHV6cbYRSVFVRftpF8/go-path"
	p2pcrypto "gx/ipfs/QmNiJiXwWE3kRhZrC5ej3kSjWHm337pYfhjLGSCDNKJP2s/go-libp2p-crypto"
	cid "gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	pubsub "gx/ipfs/QmVRxA4J3UPQpw74dLrQ6NJkfysCA1H4GU28gVpXQt9zMU/go-libp2p-pubsub"
	ipns "gx/ipfs/QmWPFehHmySCdaGttQ48iwF7M6mBRrGE5GSPWKCuMWqJDR/go-ipns"
	ipnspb "gx/ipfs/QmWPFehHmySCdaGttQ48iwF7M6mBRrGE5GSPWKCuMWqJDR/go-ipns/pb"
	proto "gx/ipfs/QmdxUuburamoF6zF9qjeQC4WYcWGbWuRmdLacMEsW8ioD8/gogo-protobuf/proto"
	multibase "gx/ipfs/QmekxXDhCxCJRNuzmHreuaT3BsuJcsjcXWNrtV9C8DRHtd/go-multibase"
	multihash "gx/ipfs/QmerPMzPk1mJVowm8KgmoknWa4yCYvvugMPsgWmDNUvDLW/go-multihash"
)

type Namesys struct {
	PubSub *pubsub.PubSub
	DNS    *net.Resolver
	Topic  string
}

func NewNamesys(psub *pubsub.PubSub, resolver *net.Resolver, topic string) *Namesys {
	nsys := &Namesys{psub, resolver, topic}
	return nsys
}

func (n *Namesys) Resolve(ctx context.Context, namepath string, opts ...namesysopt.ResolveOpt) (path.Path, error) {
	if !strings.HasPrefix(namepath, "/ipns/") {
		return "", fmt.Errorf("not an ipns name: %s", namepath)
	}

	peerid, err := multihash.FromB58String(strings.Split(namepath, "/")[2])
	if err != nil {
		return "", fmt.Errorf("failed to decode PeerID: %s", err)
	}

	peercid := cid.NewCidV1(cid.Raw, peerid)
	peeridb32 := peercid.Encode(multibase.MustNewEncoder(multibase.Base32))
	domainname := peeridb32 + ".lars.pub"

	fmt.Printf("dns: lookup: TXT %s\n", domainname)
	records, err := n.DNS.LookupTXT(ctx, domainname)
	if err != nil {
		return "", err
	}

	str := strings.Join(records, "")

	// fmt.Printf("dns: record: %+v\n", str)
	if !strings.HasPrefix(str, "ipns=") || len(str) <= 5 {
		return "", fmt.Errorf("dns: not a ipns= record")
	}
	// fmt.Printf("dns: multibase\n")
	_, pb, err := multibase.Decode(str[5:])
	if err != nil {
		return "", fmt.Errorf("dns: multibase error: %s", err)
	}

	// fmt.Printf("dns: protobuf\n")
	entry := new(ipnspb.IpnsEntry)
	err = proto.Unmarshal(pb, entry)
	if err != nil {
		return "", fmt.Errorf("dns: protobuf error: %s", err)
	}
	// fmt.Printf("dns: path\n")
	p, err := path.ParsePath(string(entry.GetValue()))
	if err != nil {
		return "", fmt.Errorf("dns: path error: %s", err)
	}

	// fmt.Printf("dns: return %s\n", p)
	return p, nil
}

func (n *Namesys) ResolveAsync(ctx context.Context, name string, opts ...namesysopt.ResolveOpt) <-chan namesys.Result {
	res := make(chan namesys.Result, 1)
	path, err := n.Resolve(ctx, name, opts...)
	res <- namesys.Result{path, err}
	close(res)
	return res
}

func (n *Namesys) Publish(ctx context.Context, name p2pcrypto.PrivKey, value path.Path) error {
	arbitraryEOL := time.Now().Add(24 * time.Hour)
	return n.PublishWithEOL(ctx, name, value, arbitraryEOL)
}

func (n *Namesys) PublishWithEOL(ctx context.Context, privkey p2pcrypto.PrivKey, value path.Path, eol time.Time) error {
	seqNo := 0
	entry, err := ipns.Create(privkey, []byte(value), uint64(seqNo), eol)
	if err != nil {
		return err
	}

	if err = ipns.EmbedPublicKey(privkey.GetPublic(), entry); err != nil {
		return err
	}

	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}

	fmt.Printf("publishing to pubsub...\n")
	if err = n.PubSub.Publish(n.Topic, data); err != nil {
		return err
	}
	fmt.Printf("publishing to pubsub: done\n")

	return nil
}
