package honlog

import "fmt"

type ErrNodeRedundant struct {
	key string
}

func (e *ErrNodeRedundant) Error() string {
	return fmt.Sprintf("Node %s already appended.", e.key)
}
