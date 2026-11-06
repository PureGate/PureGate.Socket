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
- Fire-and-forget event frames, handled in order per connection by default (`EventOrdering`).
- Wire-level event batching.
- Checked event batching with per-item results.
- Heartbeat and idle timeout.
- Request timeouts.
- Request cancellation frames.
- Remote request deadline enforcement.
- Maximum frame size enforcement.
- Transparent chunking for payloads larger than one frame.
- Chunked message identity, timeout, and in-progress count limits.
- A per-connection memory budget (`MaxConnectionMemoryBytes`) over everything held for the remote.
- Optional Brotli frame payload compression, decompressed off the read loop with pooled buffers.
- Decompressed payload size enforcement.
- Bounded handler concurrency and queueing.
- Pluggable payload codec that writes into pooled buffers and reads received bytes in place; source-generated JSON for AOT.
- Pluggable handshake authentication.
- Pluggable request/event authorization.
- Pluggable stream filter for TLS/mTLS or other transport wrapping.
- Built-in TLS/mTLS stream filter.
- Metrics through `System.Diagnostics.Metrics` (meter `PureGate.Socket`).
- Logging observer hooks.
- Composite observer for combining registry, metrics, and logging.
- Accept/handshake failure observer hook.
- Graceful close frame on peer disposal.
- Reconnecting single-peer client wrapper, by IP address or host name (resolved on every reconnect).
- Fast detection of half-open connections: a request timeout triggers a liveness ping (`LivenessProbeTimeout`).
- Optional background keep-connected client loop with bounded jittered backoff.
- Explicit idempotent request retry policy.
- Protocol-level idempotency keys.
- Shared idempotency/replay cache with TTL and size bounds.
- Queue-indexed replay-cache pruning without full-cache scans.
- Strict malformed-frame rejection.
- Credit-based flow control: per-connection message credits and per-stream byte windows that grow automatically when they limit throughput.
- Stream-specific authorization.
- Allocation-tight stream chunk writes.
- Live peer registry for multi-peer services, including multiple sessions per node.
- Peer selection policies: first, round-robin, random.

## Flow control

Each side controls how much the other may send it:

- **Messages.** A peer may have at most as many requests, events and stream requests waiting at the remote as the remote's handler queue holds (`MaxQueuedHandlers`, exchanged in the handshake). When the remote is busy, `RequestAsync`, `OpenStreamAsync` and `SendEventAsync` wait for room, within their timeout or cancellation token, instead of failing or overrunning the remote. The local `MaxInFlightRequests` limit still fails fast with `overloaded`.
- **Streams.** A producer sends only what the consumer has room for: `StreamWindowBytes` to start (1 MB by default), with more granted as the application reads. When the consumer runs out of data while the producer is waiting for room, the window doubles, up to `MaxStreamWindowBytes` (64 MB by default). A slow or stalled reader holds back only its own stream, never the connection.

Because nothing a peer sends can exceed what it was granted, the connection's read loop never waits, and two peers flooding each other cannot deadlock. A peer that sends more than it was granted is closed.

## Serialization

A codec turns message values into payload bytes and back:

```csharp
public interface ISocketMessageCodec
{
  void Serialize<T>(T value, IBufferWriter<byte> output);
  T Deserialize<T>(ReadOnlySequence<byte> payload);
}
```

Typed calls (`RequestAsync<TRequest, TResponse>`, `SendEventAsync<T>`, `OpenStreamAsync<T>`, `SocketClient`) serialize into a buffer rented from the shared array pool and return it once the frame has been sent, so they allocate no payload array. `RequestAsync<TRequest, TResponse>` also reads the response into a pooled buffer, because it only deserializes it, and returns the buffer straight after. `SocketClient` serializes once and reuses the buffer for every retry. Received payloads are read in place by `ReadPayload<T>(codec)`. The `byte[]` overloads remain for callers that already have bytes. A typed result is `null` when the payload is JSON `null`.

`SocketResponse.Success(codec, value)` serializes when the response is sent, straight into a pooled buffer, so the value must not change after the handler returns. A value the codec cannot serialize fails that request with `handler_failed`. A response kept for idempotent retries, or whose `Payload` is read, is serialized once into an array instead.

`JsonSocketMessageCodec.Shared` uses System.Text.Json with web defaults and reflection. For AOT and trimming, pass a source-generated context, which must register every message type:

```csharp
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(Order))]
partial class AppJsonContext : JsonSerializerContext;

var options = new SocketFabricOptions { Codec = new JsonSocketMessageCodec(AppJsonContext.Default) };
```

The handshake has its own source-generated JSON, so it needs no reflection whatever codec is configured. `RawSocketMessageCodec` carries `byte[]`, `ReadOnlyMemory<byte>`, `Memory<byte>`, `ArraySegment<byte>` and strings (UTF-8). For one-off use, `codec.Serialize(value)` returns an array and `codec.Deserialize<T>(memory)` reads one.

JSON is the default because it needs no dependency, but it dominates the cost of a typed call: measured with a ~700-byte object, about 60% of the time went to System.Text.Json, and neither source generation nor stricter options changed that. A MessagePack codec made the same calls about 28% faster with 35% fewer bytes allocated. It takes a dozen lines plus the `MessagePack` package in your own project, so PureGate.Socket stays dependency-free (the test suite runs this exact codec):

```csharp
using System.Buffers;
using MessagePack;
using PureGate.Socket;

public sealed class MessagePackSocketMessageCodec : ISocketMessageCodec
{
    public static readonly MessagePackSocketMessageCodec Shared = new();

    public void Serialize<T>(T value, IBufferWriter<byte> output) => MessagePackSerializer.Serialize(output, value);

    public T Deserialize<T>(ReadOnlySequence<byte> payload) => MessagePackSerializer.Deserialize<T>(payload);
}
```

Its types are annotated with `[MessagePackObject]` and `[Key]`. Pass `MessagePackSerializerOptions` to both calls for contractless types, LZ4 compression or a source-generated resolver for AOT.

## Socket buffers

`SocketBufferBytes` sets each connection's kernel send and receive buffers, before the connection is established so TCP can scale its window to them. The kernel uses that memory only for data actually buffered. The default is 1 MB on Windows. Its own defaults made each socket write wait for the remote to drain the previous one: measured on loopback, 1 MB messages from 16 callers ran 9× faster and 64 KB messages from 64 callers 3× faster with 1 MB buffers. Elsewhere the default is 0, which keeps the operating system's setting: Linux tunes its buffers automatically, and setting them explicitly turns that tuning off.

## Memory budget

Credits bound how many messages a peer can queue and windows bound each stream, but not their sizes added up. `MaxConnectionMemoryBytes` (256 MB by default) caps the total one connection holds for its remote: chunked messages being reassembled, received requests, stream requests and events until their handler finishes, decompressed payloads, and stream data the application has not read yet. Responses are not counted once they reach the caller.

A message that does not fit is refused without closing the connection where its kind allows:

- A request or stream request is answered with `overloaded`; the sender can retry.
- A stream whose data does not fit fails with `overloaded` and its producer is told to stop. Other streams continue.
- A response that does not fit fails its request with `overloaded`.
- An event closes the connection with `overloaded`, because closing is the only signal an event's sender gets.

Compressed payloads are decompressed where they are used, never on the read loop: requests, stream requests and events by the handler worker before the handler runs, responses on the caller's thread. Stream data is decompressed as it arrives, but never past the bytes the producer may still send. Decompression is bounded by `MaxDecompressedPayloadSizeBytes` and by the budget, so a small compressed payload cannot expand into unbounded work or memory.

## Metrics

Metrics are published through `System.Diagnostics.Metrics` under the meter `PureGate.Socket` (`SocketMetrics.MeterName`). Collect them with OpenTelemetry (`.AddMeter("PureGate.Socket")`), `dotnet-counters monitor --counters PureGate.Socket`, or a `MeterListener`. Nothing is measured until a listener subscribes.

| Instrument | Unit | Tags |
|---|---|---|
| `puregate.socket.client.request.duration` | s | `rpc.service`, `rpc.method`, `error.type` |
| `puregate.socket.server.request.duration` | s | `rpc.service`, `rpc.method`, `error.type` |
| `puregate.socket.events.sent`, `puregate.socket.events.received` | {event} | `rpc.service`, `rpc.method` |
| `puregate.socket.connections` | {connection} | |
| `puregate.socket.connections.closed` | {connection} | `error.type` when the connection failed |
| `puregate.socket.handshake.failures` | {handshake} | `role` (client, server), `error.type` |
| `puregate.socket.bytes.sent`, `puregate.socket.bytes.received` | By | |
| `puregate.socket.requests.in_flight` | {request} | |
| `puregate.socket.handlers.queued`, `puregate.socket.handlers.active` | {message}, {worker} | |
| `puregate.socket.streams.active` | {stream} | `direction` (in, out) |
| `puregate.socket.memory.used` | By | |
| `puregate.socket.memory.rejections` | {message} | `message.kind` |
| `puregate.socket.flow_control.waits` | {wait} | |
| `puregate.socket.liveness.failures` | {connection} | |

Request durations use buckets from 50 µs to 10 s. `error.type` is the error code (for example `timeout` or `overloaded`) and is absent on success.

## Security

The default authenticator is `NoSocketAuthenticator` for local development and tests.

Production callers should provide an authenticator such as `HmacSocketAuthenticator`, which uses challenge-response transcript signing. Callers can wrap the transport with TLS/mTLS by providing an `ISocketStreamFilter` without changing higher-level protocol services. `TlsSocketStreamFilter` is included for standard `SslStream`-based TLS/mTLS.

Identity:

- **Per-node secrets.** `new HmacSocketAuthenticator(string)` uses one secret for the whole cluster, so any node that holds it can claim any node ID. `new HmacSocketAuthenticator(Func<string, byte[]>)` looks up the secret shared with each remote node (return `null` for unknown nodes). A node can then only authenticate as an ID whose secret it holds. The lookup must be symmetric: A's lookup of `B` and B's lookup of `A` return the same bytes. An unknown node fails exactly like a wrong secret, so the handshake does not reveal which node IDs exist.
- **Certificate binding.** With `TlsSocketStreamFilter.Create(..., bindNodeIdToCertificate: true)` (or a custom `RemoteNodeValidation`), a connection is accepted only if the remote certificate names the node ID the handshake authenticated, as a DNS subject alternative name. On a listener this needs `requireClientCertificate: true`. Both sides check: a client also refuses a server whose certificate does not name the server's node ID. Other stream filters can check identity by implementing `ISocketStreamFilter.VerifyRemoteNodeAsync`.
- **Idempotency** keys are scoped by the authenticated remote node ID, so one node's key never returns another node's cached response.

Limits before authentication:

- Handshake frames are limited to `MaxHandshakeFrameBytes` (16 KB by default). The full `MaxFrameSizeBytes` applies only after the handshake succeeds.
- A listener closes new connections at once while `MaxPendingHandshakes` (256 by default) are still handshaking, or while established plus handshaking connections reach `MaxConnections` (unlimited by default). Each rejection is reported to `ISocketObserver.PeerAcceptFailed` with `overloaded`.
- A handshake that does not finish within `HandshakeTimeout` is closed.

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
- `Observability`: observer hooks and observer composition, and `SocketMetrics`.
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
