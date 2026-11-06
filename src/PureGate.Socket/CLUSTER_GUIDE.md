# PureGate.Socket Cluster Guide

This guide describes the socket fabric from the perspective of future PureGate cluster code. It intentionally avoids cluster membership, storage, routing, or replication policy. Those belong above the socket layer.

## Core Model

Every cluster node should be able to listen for inbound socket peers and create outbound socket clients.

`SocketPeer` is bidirectional. Once two nodes share a connected peer, either side can send requests, events, batches, and streams over the same TCP connection. Cluster code does not need a separate request connection and response connection.

`SocketClient` is the lifecycle helper for a single remote endpoint. By default it reconnects lazily when a caller asks for a peer. For long-lived cluster links, enable `SocketClientOptions.KeepConnected` so the client maintains the link in the background with bounded jittered backoff.

## Link Semantics

A connected peer means:
- Both sides completed protocol handshake.
- Both sides know the remote `NodeId`.
- Heartbeat and idle timeout are active.
- Either side can initiate request/response work.
- Either side can send fire-and-forget events.
- Either side can open pull streams.

A closed peer means:
- Pending requests complete with connection-closed semantics.
- Pending streams fail or complete.
- The peer should be removed from any live routing table.
- A keep-connected client may create a replacement peer.

## Recommended Topology Use

Prefer one logical link per node pair. Two nodes may temporarily create duplicate physical connections during startup or reconnect races, but cluster code should converge to one preferred peer when it wants a clean topology.

If cluster code deliberately keeps two physical connections per pair, treat them as two independent sessions. Do not assume they share ordering, backpressure, or failure state.

## Operations On The Link

Use normal requests for small command/reply interactions:
- node status
- key lookup
- metadata query
- lease or ownership checks

Use events for notifications where the sender does not need a response:
- cache invalidation
- deployment available
- local route changed
- peer heartbeat extensions above the transport layer

Use checked event batches when the sender needs per-item acceptance without modeling each item as a full RPC.

Use request batches when many small commands should cross the wire together and each item needs a result.

Use streams for large payload transfer:
- artifact chunks
- blob transfer
- snapshot export/import
- large ZoneTree page or segment movement

## Retry And Idempotency

Reconnect is not replay.

`SocketClient` may reconnect automatically, but it does not automatically repeat unsafe work. Request replay is explicit through `SocketClientRequestOptions`:
- Set `Idempotent = true` only for operations that are safe to replay.
- Provide a stable `IdempotencyKey` when the caller already has an operation id.
- Let the client generate a key only when one logical call owns all attempts.

The protocol-level idempotency cache can return a completed response if the remote side already executed the operation before the connection dropped.

## Observability Contract

Cluster code should plug an observer into `SocketFabricOptions.Observer`.

Useful cluster-facing signals include:
- `PeerConnected`
- `PeerDisconnected`
- `PeerAcceptFailed`
- `ListenerAcceptFailed`
- `ClientConnectStarted`
- `ClientConnectFailed`
- `ClientReconnectScheduled`
- `ClientKeepConnectedLoopFailed`
- `RequestStarted`
- `RequestCompleted`

Observer callbacks are isolated. They must be safe to fail without breaking socket traffic.

## Security Surface

The default no-auth mode is for local development and tests.

Production cluster links should choose a real authentication and transport posture:
- `HmacSocketAuthenticator` for shared-secret challenge-response.
- `TlsSocketStreamFilter` when the deployment wants TLS or mTLS.
- `ISocketAuthorizer` for service/operation-level authorization after authentication.

The socket layer authenticates peers and authorizes messages. Cluster code still owns membership trust, node admission, and operational policy.

## Backpressure Boundaries

The fabric already bounds:
- max frame size
- max decompressed payload size
- max in-flight requests
- handler concurrency
- handler queue length
- incoming chunked message count
- inbound stream buffering

Cluster services should still design their own domain limits. Socket limits protect the transport; they do not decide storage quotas, replication windows, or membership safety.

## Clean Separation

PureGate.Socket should remain generic.

Good socket-level concepts:
- peers
- frames
- requests
- events
- streams
- auth
- reconnect
- observability

Cluster-level concepts should stay above it:
- ZoneTree partitions
- rendezvous hashing
- replication policy
- deployment state
- blob placement
- node roles
- membership changes

The socket layer gives PureGate a fast, bidirectional, resilient fabric. The cluster layer decides what the messages mean.
