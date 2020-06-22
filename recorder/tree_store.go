package recorder

import (
	honlog "github.com/SJTU-OpenNetwork/hon-textile/hon-log"
	"sync"
)

type streamTree struct {
	tree *honlog.Tree
	streamId string
}

type treeCache struct {
	cache []*streamTree
	quickindex map[string]*streamTree
	capacity int
	writeHead int
	selfId string
	lock sync.Mutex
}

func NewTreeCache(capacity int, selfId string) *treeCache {
	return &treeCache{
		cache: make([]*streamTree, capacity),
		capacity: capacity,
		quickindex: make(map[string]*streamTree),
		writeHead: 0,
		selfId: selfId,
	}
}

func (c *treeCache) Add(streamId string, parentId string, peerId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	t, ok := c.quickindex[streamId]
	if ok {
		return t.tree.Append(peerId, nil, parentId)
	} else {
		if c.cache[c.writeHead] != nil {
			delete(c.quickindex, c.cache[c.writeHead].streamId)
		}
		newTree := honlog.NewTree(c.selfId, nil)
		newStreamTree := &streamTree{
			tree:    newTree,
			streamId: streamId,
		}
		c.cache[c.writeHead] = newStreamTree
		c.quickindex[streamId] = newStreamTree
		c.writeHead = (c.writeHead+1)%c.capacity
		err := newTree.Append(peerId, nil, parentId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *treeCache) Get(streamId string) *honlog.Tree {
	c.lock.Lock()
	c.lock.Unlock()
	t, ok := c.quickindex[streamId]
	if ok {
		return t.tree
	} else {
		return nil
	}
}