package core

import (
	"context"
	"errors"
	"strings"

	namesys "github.com/ipfs/go-ipfs/namesys"

	path "gx/ipfs/QmVi2uUygezqaMTqs3Yzt5FcZFHJoYD4B7jQ2BELjj7ZuY/go-path"
	resolver "gx/ipfs/QmVi2uUygezqaMTqs3Yzt5FcZFHJoYD4B7jQ2BELjj7ZuY/go-path/resolver"
	ipld "gx/ipfs/QmcKKBwfz6FyQdHR2jsXrrF6XeSBXYL86anmWNewpFpoF5/go-ipld-format"
)

// ErrNoNamesys is an explicit error for when an IPFS node doesn't
// (yet) have a name system
var ErrNoNamesys = errors.New(
	"core/resolve: no Namesys on IpfsNode - can't resolve ipns entry")

// ResolveIPNS resolves /ipns paths
func ResolveIPNS(ctx context.Context, nsys namesys.NameSystem, p path.Path) (out path.Path, err error) {
	ctx = log.Start(ctx, "ResolveIPNS")
	defer func() { log.FinishWithErr(ctx, err) }()
	if strings.HasPrefix(p.String(), "/ipns/") {
		// resolve ipns paths

		// TODO(cryptix): we should be able to query the local cache for the path
		if nsys == nil {
			return "", ErrNoNamesys
		}

		seg := p.Segments()

		if len(seg) < 2 || seg[1] == "" { // just "/<protocol/>" without further segments
			return "", path.ErrNoComponents
		}

		extensions := seg[2:]
		resolvable, err := path.FromSegments("/", seg[0], seg[1])
		if err != nil {
			return "", err
		}

		respath, err := nsys.Resolve(ctx, resolvable.String())
		if err != nil {
			return "", err
		}

		segments := append(respath.Segments(), extensions...)
		p, err = path.FromSegments("/", segments...)
		if err != nil {
			return "", err
		}
	}
	return p, nil
}

// Resolve resolves the given path by parsing out protocol-specific
// entries (e.g. /ipns/<node-key>) and then going through the /ipfs/
// entries and returning the final node.
func Resolve(ctx context.Context, nsys namesys.NameSystem, r *resolver.Resolver, p path.Path) (out ipld.Node, err error) {
	ctx = log.Start(ctx, "Resolve")
	log.SetTag(ctx, "path", p.String())
	defer func() { log.FinishWithErr(ctx, err) }()
	p, err = ResolveIPNS(ctx, nsys, p)
	if err != nil {
		return nil, err
	}

	// ok, we have an IPFS path now (or what we'll treat as one)
	return r.ResolvePath(ctx, p)
}
