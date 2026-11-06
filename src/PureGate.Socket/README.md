# PureGate.Socket

PureGate.Socket is a raw TCP fabric for long-lived peer connections.

It is intentionally independent from PureGate application concepts. It does not know about ZoneTree, blobs, mail, auth users, deployments, or cluster membership. Those become services on top of the fabric.

See [CLUSTER_GUIDE.md](CLUSTER_GUIDE.md) for the socket-side contract future PureGate cluster code should build on.

## Shape

- Length-prefixed binary TCP frames.
- Raw payload bytes without JSON/base64 envelope overhead.
- Pipe-backed frame reading and writing.
- Bidirectional request/response.
- Pull-based streaming requests.
- Wire-level request batching.
- Fire-and-forget event frames.
- Wire-level event batching.
- Checked event batching with per-item results.
- Heartbeat and idle timeout.
- Request timeouts.
- Request cancellation frames.
- Remote request deadline enforcement.
- Maximum frame size enforcement.
- Transparent chunking for payloads larger than one frame.
- Chunked message identity, timeout, and in-progress count limits.
- Optional Brotli frame payload compression.
- Decompressed payload size enforcement.
- Bounded handler concurrency and queueing.
- Pluggable payload codec.
- Pluggable handshake authentication.
- Pluggable request/event authorization.
- Pluggable stream filter for TLS/mTLS or other transport wrapping.
- Built-in TLS/mTLS stream filter.
- Metrics/logging observer hooks.
- Composite observer for combining registry, metrics, and logging.
- Accept/handshake failure observer hook.
- Graceful close frame on peer disposal.
- Reconnecting single-peer client wrapper.
- Optional background keep-connected client loop with bounded jittered backoff.
- Explicit idempotent request retry policy.
- Protocol-level idempotency keys.
- Shared idempotency/replay cache with TTL and size bounds.
- Queue-indexed replay-cache pruning without full-cache scans.
- Strict malformed-frame rejection.
- Bounded inbound stream buffering.
- Stream-specific authorization.
- Allocation-tight stream chunk writes.
- Live peer registry for multi-peer services, including multiple sessions per node.
- Peer selection policies: first, round-robin, random.

## Security

The default authenticator is `NoSocketAuthenticator` for local development and tests.

Production callers should provide an authenticator such as `HmacSocketAuthenticator`, which uses challenge-response transcript signing. Callers can wrap the transport with TLS/mTLS by providing an `ISocketStreamFilter` without changing higher-level protocol services. `TlsSocketStreamFilter` is included for standard `SslStream`-based TLS/mTLS.

`ISocketAuthorizer` can reject individual requests, stream requests, or events after the peer is authenticated. Denied requests and stream requests receive an `unauthorized` response; denied events close the peer because events are fire-and-forget.

## Services

The socket fabric uses `service` and `operation` names in every request and event. PureGate cluster features should be layered on top as services, for example:
- `kv.get`, `kv.upsert`, `kv.list`
- `events.publish`, `events.subscribe`
- `blob.have`, `blob.pull`
- `node.status`, `node.drain`

The transport stays generic; PureGate owns the meaning of each service.

Batching uses an internal compact binary envelope and preserves each item result independently. Batch items are dispatched through the same authorization and handler path as normal requests and events. Event batches can use either the fire-and-forget path or the checked request/response path when callers need per-item apply results.

Streaming uses `StreamRequest`, `StreamStart`, `StreamItem`, and `StreamEnd` frames. Callers receive a normal `Stream`; remote failures surface while reading, and disposing the returned stream sends cancellation to the producer. Stream chunk writes use the rented copy buffer directly, avoiding a per-chunk payload array on the producer hot path.

Idempotent request retries carry a protocol-level idempotency key. If a connection drops after the remote handler already produced a response, the reconnecting caller receives the cached response instead of executing the handler again.

`SocketClient` reconnects lazily on demand by default. Cluster links can opt into `SocketClientOptions.KeepConnected` to maintain a live peer in the background. Failed attempts use bounded exponential backoff with jitter; request replay still remains explicit through `SocketClientRequestOptions` so reconnect never hides unsafe duplicate writes.

Frame reading and writing use `System.IO.Pipelines` internally. The public API stays stream-shaped, while frame parsing and encoding use pipe buffers on the hot path.

## Layers

- `Transport`: raw peers, listeners, connectors, and stream filters.
- `Protocol`: frame-level options, message kinds, errors, requests, and responses.
- `Security`: handshake authentication and per-message authorization.
- `Serialization`: payload codecs.
- `Observability`: observer hooks and observer composition.
- `Fabric`: lifecycle and topology helpers such as reconnecting clients and peer registries.

## Benchmarks

Run the dependency-free benchmark harness in Release mode:

```bash
dotnet run -c Release --project PureGate.Socket.Benchmarks -- --iterations 5000 --clients 8 --stream-bytes 33554432
```

The harness measures frame roundtrip, RPC echo, request batching, pull streaming, and concurrent RPC pressure. It reports elapsed time, throughput, allocation deltas, and GC collection counts.

Options:

- `--suite classic|matrix|all` (default `all`). The matrix suite adds a raw-socket ceiling, p50/p99/p99.9 latency, payloads from 64 B to 1 MB, 1/8/64 connections, and TLS and Brotli variants.
- `--json <path>` writes the matrix results as JSON. The Phase 0 baseline is in `docs/benchmarks/baseline-phase0.json`.
- `--quick` runs a reduced version, as CI does.

Compare runs on the same machine, and compare medians of several runs: run-to-run noise on concurrent scenarios can exceed 30%.
