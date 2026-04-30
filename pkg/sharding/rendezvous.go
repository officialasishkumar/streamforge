package sharding

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

type Node struct {
	ID     string
	Weight uint32
}

type Ring struct {
	nodes []Node
}

func New(nodes []Node) (*Ring, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("sharding: at least one node is required")
	}

	seen := make(map[string]struct{}, len(nodes))
	copied := make([]Node, len(nodes))
	for i, node := range nodes {
		if node.ID == "" {
			return nil, fmt.Errorf("sharding: node id is required")
		}
		if node.Weight == 0 {
			return nil, fmt.Errorf("sharding: node %q weight must be positive", node.ID)
		}
		if _, ok := seen[node.ID]; ok {
			return nil, fmt.Errorf("sharding: duplicate node id %q", node.ID)
		}
		seen[node.ID] = struct{}{}
		copied[i] = node
	}

	sort.Slice(copied, func(i, j int) bool {
		return copied[i].ID < copied[j].ID
	})
	return &Ring{nodes: copied}, nil
}

func (r *Ring) Pick(key string) (Node, bool) {
	nodes := r.PickN(key, 1)
	if len(nodes) == 0 {
		return Node{}, false
	}
	return nodes[0], true
}

func (r *Ring) PickN(key string, n int) []Node {
	if r == nil || n <= 0 || len(r.nodes) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	scored := make([]scoredNode, len(r.nodes))
	for i, node := range r.nodes {
		scored[i] = scoredNode{
			node:  node,
			score: weightedScore(key, node),
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			return scored[i].node.ID < scored[j].node.ID
		}
		return scored[i].score > scored[j].score
	})

	picked := make([]Node, n)
	for i := 0; i < n; i++ {
		picked[i] = scored[i].node
	}
	return picked
}

type scoredNode struct {
	node  Node
	score float64
}

func weightedScore(key string, node Node) float64 {
	sum := sha256.Sum256([]byte(key + "\x00" + node.ID))
	raw := binary.BigEndian.Uint64(sum[:8])
	u := (float64(raw) + 1) / (float64(math.MaxUint64) + 1)

	return math.Pow(u, 1/float64(node.Weight))
}
