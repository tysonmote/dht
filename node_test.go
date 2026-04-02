package dht

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stvp/tempconsul"
)

func apiClient() (*api.Client, error) {
	return api.NewClient(api.DefaultConfig())
}

func startConsul() (server *tempconsul.Server, err error) {
	server = &tempconsul.Server{}
	return server, server.Start()
}

func servicesCount(name string) (count int, err error) {
	client, err := apiClient()
	if err != nil {
		return 0, err
	}
	services, _, err := client.Catalog().Service(name, "", nil)
	return len(services), err
}

func TestJoinLeave(t *testing.T) {
	// No Consul agent
	_, err := Join("test", "a")
	if err == nil {
		t.Errorf("expected error got nil")
	}

	// Start Consul agent
	server, err := startConsul()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Term()

	// Valid join
	node, err := Join("test", "a")
	if err != nil {
		t.Fatal(err)
	}

	count, err := servicesCount("test")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 service registered, got %d", count)
	}

	// Leave
	err = node.Leave()
	if err != nil {
		t.Error(err)
	}

	count, err = servicesCount("test")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 service registered, got %d", count)
	}

	// Idempotent Leave
	if err := node.Leave(); err != nil {
		t.Errorf("second Leave: %v", err)
	}
}

func TestMember(t *testing.T) {
	server, err := startConsul()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Term()

	n := 3
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		nodes[i], err = Join("test", strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ensure nodes have the latest state. First we have to wait for consul
	// to run all checks so that all services are "passing".
	time.Sleep(checkInterval * 2)
	for _, node := range nodes {
		if err := node.refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	keys := []string{"", "a", "b", "d9edf13e917c4f0f66be0e80cc30060e", "a2a9538886f1df96be9e5b52b14b404a"}
	for k := 0; k < 50; k++ {
		keys = append(keys, strconv.Itoa(k))
	}

	for _, key := range keys {
		var owners int
		for _, node := range nodes {
			if node.Member(key) {
				owners++
			}
		}
		if owners != 1 {
			t.Errorf("Member(%#v): want exactly one owner among 3 nodes, got %d", key, owners)
		}
	}

	// Clean up
	for _, node := range nodes {
		err = node.Leave()
		if err != nil {
			t.Error(err)
		}
	}
}
