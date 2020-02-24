package ipfs

import (
	"context"
    "time"

	"github.com/SJTU-OpenNetwork/go-stream"
	"github.com/SJTU-OpenNetwork/go-ipfs/core"
	"github.com/SJTU-OpenNetwork/go-ipfs/core/coreapi"
)


const StreamTimeout = time.Second * 1

func StartStream(node *core.IpfsNode, s *stream.Stream) error {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(node.Context(), StreamTimeout)
	defer cancel()
	err = api.Stream().StartStream(ctx, s)
    return err
}


//call go-stream StartWorker
func StartWorker(node *core.IpfsNode, s *stream.StreamConfig, peerid peer.ID) {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(node.Context(), StreamTimeout)
	defer cancel()
	err = api.Stream().StartWorker(ctx, s, peerid)
    return err

}

// [deprecated]
func AddStreamBlock(node *core.IpfsNode, b *stream.StreamBlock) error {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(node.Context(), StreamTimeout)
	defer cancel()
	err = api.Stream().AddStreamBlock(ctx, b)
    return err
}

func SubscribeStream(node *core.IpfsNode, conf *stream.StreamConfig) error {
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(node.Context(), StreamTimeout)
	defer cancel()
	err = api.Stream().SubscribeStream(ctx, conf)
    return err
}
