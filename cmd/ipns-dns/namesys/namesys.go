package dnspubsub

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	namesys "github.com/ipfs/go-ipfs/namesys"
	namesysopt "github.com/ipfs/go-ipfs/namesys/opts"

	cid "gx/ipfs/QmPSQnBKM9g7BaUcZCvswUJVscQ1ipjmwxN5PXCjkp9EQ7/go-cid"
	multihash "gx/ipfs/QmPnFwZ2JXKnXgMw8CdBPxn7FWh6LLdjUjxV1fKHuJnkr8/go-multihash"
	p2pcrypto "gx/ipfs/QmPvyPwuCgJ7pDmrKDxRtsScJgBaM5h4EpRL2qQJsmXf4n/go-libp2p-crypto"
	floodsub "gx/ipfs/QmTcC9Qx2adsdGguNpqZ6dJK7MMsH8sf3yfxZxG3bSwKet/go-libp2p-floodsub"
	ipns "gx/ipfs/QmX72XT6sSQRkNHKcAFLM2VqB3B4bWPetgWnHY8LgsUVeT/go-ipns"
	ipnspb "gx/ipfs/QmX72XT6sSQRkNHKcAFLM2VqB3B4bWPetgWnHY8LgsUVeT/go-ipns/pb"
	path "gx/ipfs/QmdrpbDgeYH3VxkCciQCJY5LkDYdXtig6unDzQmMxFtWEw/go-path"
	proto "gx/ipfs/QmdxUuburamoF6zF9qjeQC4WYcWGbWuRmdLacMEsW8ioD8/gogo-protobuf/proto"
	multibase "gx/ipfs/QmekxXDhCxCJRNuzmHreuaT3BsuJcsjcXWNrtV9C8DRHtd/go-multibase"
)

type Namesys struct {
	PubSub *floodsub.PubSub
	DNS    *net.Resolver
	Topic  string
}

func NewNamesys(pubsub *floodsub.PubSub, resolver *net.Resolver, topic string) Namesys {
	return Namesys{pubsub, resolver, topic}
}

func (n Namesys) Resolve(ctx context.Context, namepath string, opts ...namesysopt.ResolveOpt) (path.Path, error) {
	if !strings.HasPrefix(namepath, "/ipns/") {
		return "", fmt.Errorf("not an ipns name: %s", namepath)
	}

	peerid, err := multihash.FromB58String(strings.Split(namepath, "/")[2])
	if err != nil {
		return "", fmt.Errorf("failed to decode PeerID: %s", err)
	}

	peercid := cid.NewCidV1(cid.Raw, peerid)
	peeridb32 := peercid.Encode(multibase.MustNewEncoder(multibase.Base32))
	domainname := peeridb32 + ".ipns.name"

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

func (n Namesys) ResolveAsync(ctx context.Context, name string, opts ...namesysopt.ResolveOpt) <-chan namesys.Result {
	res := make(chan namesys.Result, 1)
	path, err := n.Resolve(ctx, name, opts...)
	res <- namesys.Result{path, err}
	close(res)
	return res
}

func (n Namesys) Publish(ctx context.Context, name p2pcrypto.PrivKey, value path.Path) error {
	arbitraryEOL := time.Now().Add(24 * time.Hour)
	return n.PublishWithEOL(ctx, name, value, arbitraryEOL)
}

func (n Namesys) PublishWithEOL(ctx context.Context, privkey p2pcrypto.PrivKey, value path.Path, eol time.Time) error {
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

	if err = n.PubSub.Publish(n.Topic, data); err != nil {
		return err
	}

	return nil
}
