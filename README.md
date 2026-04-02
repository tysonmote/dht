# dht

`dht` is a [distributed hash table][wiki_dht] implementation that uses Consul
and [rendezvous hashing][wiki_rendez] to distribute keys among distributed
nodes. Because it uses rendezvous hashing to determine key placement, removing a
node from the hash table is minimally disruptive in terms of key re-assignment.

Local hash table state is refreshed in a background goroutine using blocking
Consul API queries with the [default consistency mode][consul_api]. There is no
fixed delay between polls: each iteration issues one blocking query until the
service index changes or the agent long-poll timeout is reached. After a failed
query (for example a transient network error), the loop waits briefly before
retrying.

Errors from the background poll are logged with the `log` package in this form:

    [dht <name> <id>] error: <error message>

Pass `WithLogger` to `Join` or `JoinContext` to send those lines to a different
`log.Logger`.

`Member` may be called from multiple goroutines at the same time as the
background refresh.

`dht` requires a locally-running Consul agent (version 0.5.2 or newer) with its
HTTP API listening on `127.0.0.1:8500`. `dht` nodes run a simple HTTP server on
an available ephemeral port to allow Consul to periodically check that the node
is still alive.

[wiki_dht]: https://en.wikipedia.org/wiki/Distributed_hash_table
[wiki_rendez]: https://en.wikipedia.org/wiki/Rendezvous_hashing
[consul_api]: https://www.consul.io/docs/agent/http.html

## Example

```go
node1, err := dht.Join("worker", "worker-1")
node2, err := dht.Join("worker", "worker-2")

node1.Member("some_key") // true
node2.Member("some_key") // false

err = node1.Leave()
err = node2.Leave()
```

Use `JoinContext` for a timeout or cancellation on registration and the initial
service list fetch; use `LeaveContext` to bound deregistration. After `Join`
returns, ongoing polling is stopped with `Leave` / `LeaveContext`, not by
canceling the join context.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
node, err := dht.JoinContext(ctx, "worker", "worker-1")
```

When no passing instances exist for the service, rendezvous hashing yields no
owner: every node’s `Member` returns false for all keys until at least one
instance is healthy.
