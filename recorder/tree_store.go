package recorder

type tree_store struct {

}

type tree_node struct {
	parent *tree_node
	children []*tree_node
	data []byte
}