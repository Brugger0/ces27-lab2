package consistenthash

import (
	"errors"
	"hash/crc32"
	"sort"
	"sync"
)

var ErrNodeNotFound = errors.New("node not found")

// Ring is the data structure that holds a consistent hashed ring.
type Ring struct {
	Nodes Nodes
	sync.Mutex
}

// search will find the index of the node that is responsible for the range that
// includes the hashed value of key.
func (r *Ring) search(key string) int {
	var (
		hashValue uint32
		nodeIndex int
	)

	hashValue = hashId(key)
	nodeIndex = 0

	// the responsible node is the first one with HashId higher than the hashed value being searched for.
	for _, node := range r.Nodes {
		if node.HashId >= hashValue {
			return nodeIndex
		}

		nodeIndex++
	}

	// If no node has HashId higher the hashed value, it means that the value falls on the last part of the ring, which belongs to the first node (because of the circular ring structure).
	return 0
}

// NewRing will create a new Ring object and return a pointer to it.
func NewRing() *Ring {
	return &Ring{Nodes: Nodes{}}
}

// Verify if a node with a given id already exists in the ring and if so return
// a pointer to it.
func (r *Ring) Exists(id string) (bool, *Node) {
	r.Lock()
	defer r.Unlock()

	for _, node := range r.Nodes {
		if node.Id == id {
			return true, node
		}
	}

	return false, nil
}

// Add a node to the ring and return a pointer to it.
func (r *Ring) AddNode(id string) *Node {
	r.Lock()
	defer r.Unlock()

	node := NewNode(id)
	r.Nodes = append(r.Nodes, node)

	sort.Sort(r.Nodes)

	return node
}

// Remove a node from the ring.
func (r *Ring) RemoveNode(id string) error {
	r.Lock()
	defer r.Unlock()

	i := r.search(id)
	if i >= r.Nodes.Len() || r.Nodes[i].Id != id {
		return ErrNodeNotFound
	}

	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)

	return nil
}

// Get the id of the node responsible for the hash range of id.
func (r *Ring) Get(id string) string {
	i := r.search(id)
	if i >= r.Nodes.Len() {
		i = 0
	}

	return r.Nodes[i].Id
}

// GetNext will return the next node after the one responsible for the hash
// range of id.
func (r *Ring) GetNext(id string) (string, error) {
	r.Lock()
	defer r.Unlock()
	var i = 0
	for i < r.Nodes.Len() && r.Nodes[i].Id != id {
		i++
	}

	if i >= r.Nodes.Len() {
		return "", ErrNodeNotFound
	}

	nextIndex := (i + 1) % r.Nodes.Len()

	return r.Nodes[nextIndex].Id, nil
}

// hashId returns the hashed form of a key.
func hashId(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) % uint32(1000)
}
