package honlog

import (
	"fmt"
	"os"
	"sync"
)

// node, depth, index in children
type traverCallback func(*treeNode, int, int)error

type treeNode struct {
	parentKey string // This is useful if the children is appended before parent
	parent *treeNode
	children []*treeNode
	data []byte
	key string	// A unique key to distinguish this node
				// It is used to index nodes in a map
}

type Tree struct {
	root *treeNode
	index map[string] *treeNode
	lock sync.Mutex
}

/*
 * NewTree create a tree with a root node (key, data)
 */
func NewTree(key string, data []byte) *Tree {
	newRoot := &treeNode{
		parentKey: "",
		parent: nil,
		children: nil,
		data: data,
		key: key,
	}
	newIndex := make(map[string] *treeNode)
	newIndex[key] = newRoot

	return &Tree{
		root: newRoot,
		index: newIndex,
	}
}

/*
 * Append a new node to tree.
 * Note that the parent does not have to already on the tree.
 * Throw an error if there is already a node with that key.
 */
func (t *Tree) Append(key string, data []byte, parent string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Throw an error if this node already exists
	_, ok := t.index[key]
	if ok {
		return &ErrNodeRedundant{key: key}
	}

	// Append this node to the children list of its parent
	parNode, ok := t.index[parent]
	newNode := &treeNode{
		parentKey: parent,
		parent: parNode,
		children: nil,
		data: data,
		key: key,
	}

	if ok {
		parNode.children = append(parNode.children, newNode)
	}

	// Check if there are children of new node
	for _, n := range t.index {
		if n.parentKey == key {
			n.parent = newNode
			newNode.children = append(newNode.children, n)
		}
	}

	t.index[key] = newNode
	return nil
}

func (t *Tree) WriteCSV(filePath string) error {
	t.lock.Lock()
	f, err := os.Create(filePath)
	defer func(){
		t.lock.Unlock()
		err = f.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	}()
	if err != nil {
		return err
	}

	// Count the maximum depth of this tree
	//maxDepth, err := t.maxDepth()
	//if err != nil {
	//	return err
	//}

	cb :=  func(n *treeNode, depth int, childIndex int)error {
		if childIndex > 0 {
			for i := 0; i < depth; i++ {
				_, err = f.WriteString(",")
				if err != nil {
					return err
				}
			}
		} else if depth > 0{
			_, err = f.WriteString(",")
			if err != nil {
				return err
			}
		}
		_, err = f.WriteString(n.key)
		if err != nil {
			return err
		}
		if len(n.children)==0 {
			_, err = f.WriteString("\n")
			if err != nil {
				return err
			}
		}
		return nil
	}
	return t.root.traverse(0, cb, 0)
}

/*
 * Get the maximum depth of this tree.
 * Note that this func would not lock the Tree.
 */
func (t *Tree) maxDepth() (int, error) {
	maxDepth := 0
	cbCount := func (n *treeNode, depth int, childIndex int) error {
		if depth > maxDepth {
			maxDepth = depth
		}
		return nil
	}
	err := t.root.traverse(0, cbCount, 0)
	if err != nil {
		return 0, err
	}
	return maxDepth, nil
}

/*
 * traverse and process nodes from this node.
 * callback will be called each time reaching a node.
 */
func (node *treeNode) traverse(depth int, callback traverCallback, childIndex int) error {
	err := callback(node, depth, childIndex)
	if err != nil {
		return err
	}
	for i, child := range node.children {
		err = child.traverse(depth+1, callback, i)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
 * toRoot() return the root node of this node.
 */
func (node *treeNode) toRoot() *treeNode {
	if node.parent != nil {
		return node.parent.toRoot()
	} else {
		return node
	}
}
