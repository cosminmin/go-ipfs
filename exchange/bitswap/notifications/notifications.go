package notifications

import (
	"context"

	blocks "gx/ipfs/Qmej7nf81hi2x2tvjRBF3mcp74sQyuDH4VMYDGd1YtXjb2/go-block-format"

	pubsub "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/briantigerchow/pubsub"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	cid "gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"
)

var log = logging.Logger("bitswap/notifications")

const bufferSize = 16

type PubSub interface {
	Publish(block blocks.Block)
	Subscribe(ctx context.Context, keys ...*cid.Cid) <-chan blocks.Block
	Shutdown()
}

func New() PubSub {
	return &impl{*pubsub.New(bufferSize)}
}

type impl struct {
	wrapped pubsub.PubSub
}

func (ps *impl) Publish(block blocks.Block) {
	ps.wrapped.Pub(block, block.Cid().KeyString())
}

func (ps *impl) Shutdown() {
	ps.wrapped.Shutdown()
}

// Subscribe returns a channel of blocks for the given |keys|. |blockChannel|
// is closed if the |ctx| times out or is cancelled, or after sending len(keys)
// blocks.
func (ps *impl) Subscribe(ctx context.Context, keys ...*cid.Cid) <-chan blocks.Block {
	blocksCh := make(chan blocks.Block, len(keys))
	valuesCh := make(chan interface{}, len(keys)) // provide our own channel to control buffer, prevent blocking
	if len(keys) == 0 {
		close(blocksCh)
		return blocksCh
	}

	sKeys := toStrings(keys)
	ctx = log.EventBeginInContext(ctx, "Bitswap.Subscribe", logging.LoggableMap{"Keys": sKeys})
	ps.wrapped.AddSubOnceEach(valuesCh, sKeys...)
	go func() {
		defer func() {
			close(blocksCh)
			ps.wrapped.Unsub(valuesCh) // with a len(keys) buffer, this is an optimization
			logging.MaybeFinishEvent(ctx)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-valuesCh:
				if !ok {
					return
				}
				block, ok := val.(blocks.Block)
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case blocksCh <- block: // continue
				}
			}
		}
	}()

	return blocksCh
}

func toStrings(keys []*cid.Cid) []string {
	strs := make([]string, 0, len(keys))
	for _, key := range keys {
		strs = append(strs, key.KeyString())
	}
	return strs
}
