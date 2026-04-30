package sharding

import (
	"fmt"
	"testing"
)

func TestRingPickIsDeterministic(t *testing.T) {
	ring, err := New([]Node{
		{ID: "shard-a", Weight: 1},
		{ID: "shard-b", Weight: 1},
		{ID: "shard-c", Weight: 1},
	})
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	first, ok := ring.Pick("tenant-123")
	if !ok {
		t.Fatal("expected node")
	}
	for i := 0; i < 100; i++ {
		got, ok := ring.Pick("tenant-123")
		if !ok {
			t.Fatal("expected node")
		}
		if got != first {
			t.Fatalf("pick changed: first=%+v got=%+v", first, got)
		}
	}
}

func TestRingPickNReturnsUniqueOrderedNodes(t *testing.T) {
	ring, err := New([]Node{
		{ID: "shard-c", Weight: 1},
		{ID: "shard-a", Weight: 1},
		{ID: "shard-b", Weight: 1},
	})
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	nodes := ring.PickN("tenant-123", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected three nodes, got %d", len(nodes))
	}

	seen := map[string]bool{}
	for _, node := range nodes {
		if seen[node.ID] {
			t.Fatalf("duplicate node %q", node.ID)
		}
		seen[node.ID] = true
	}
}

func TestRingWeightsBiasDistribution(t *testing.T) {
	ring, err := New([]Node{
		{ID: "cold-shard", Weight: 1},
		{ID: "hot-shard", Weight: 8},
	})
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		node, ok := ring.Pick(fmt.Sprintf("tenant-%04d", i))
		if !ok {
			t.Fatal("expected node")
		}
		counts[node.ID]++
	}

	if counts["hot-shard"] <= counts["cold-shard"] {
		t.Fatalf("expected weighted shard to receive more keys, got hot=%d cold=%d", counts["hot-shard"], counts["cold-shard"])
	}
}

func TestRingRejectsInvalidNodes(t *testing.T) {
	tests := []struct {
		name  string
		nodes []Node
	}{
		{name: "empty ring"},
		{name: "empty id", nodes: []Node{{Weight: 1}}},
		{name: "zero weight", nodes: []Node{{ID: "shard-a"}}},
		{name: "duplicate id", nodes: []Node{{ID: "shard-a", Weight: 1}, {ID: "shard-a", Weight: 1}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.nodes); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
