# PureGate.Socket

[![NuGet](https://img.shields.io/nuget/v/PureGate.Socket.svg)](https://www.nuget.org/packages/PureGate.Socket)
[![CI](https://github.com/PureGate/PureGate.Socket/actions/workflows/ci.yml/badge.svg)](https://github.com/PureGate/PureGate.Socket/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/PureGate/PureGate.Socket/blob/main/LICENSE)

A fast, dependency-free messaging layer for long-lived TCP connections between .NET services.

PureGate.Socket gives two processes one connection that both of them can use: either side can call the other, send it events, or stream data to it. Every message is framed, multiplexed and flow-controlled, so many concurrent calls share one socket without blocking each other or overrunning the receiver. There's no HTTP, no code generation and no runtime dependency beyond .NET itself.

```
dotnet add package PureGate.Socket
```

Requires .NET 10.

## Why use it

Raw sockets give you a byte stream. Everything a real service needs on top of that is the hard part, and it's what this library does:

- **Calls, events and streams on one connection.** Typed request/response, fire-and-forget events, batches and pull-based binary streams, all multiplexed. Both ends are peers, so a server can call its clients as easily as they call it.
- **Backpressure that can't deadlock.** Senders wait when the receiver's queue is full instead of overrunning it. Each stream has its own window that grows to fit the link, so a slow reader holds back only its own stream. Two peers flooding each other keep making progress.
- **Bounded memory.** Each connection has a memory budget covering everything it holds for the remote. A message that doesn't fit is refused cleanly instead of buffered until the process runs out of memory, and compressed payloads can't expand past their limits.
- **Failure handling built in.** Timeouts that travel with the request, cancellation that reaches the remote handler, half-open connection detection, a reconnecting client, and retries with an idempotency cache so a retried call never runs twice.
- **Security.** Mutual HMAC authentication with a secret per node pair, TLS and mutual TLS with node IDs bound to certificates, per-message authorization, and limits that hold before a peer has authenticated.
- **Production visibility.** Metrics through `System.Diagnostics.Metrics`, ready for OpenTelemetry: request latency, connections, bytes, queues, memory and flow control.
- **Cheap on the wire and in memory.** After a method's first use, a request costs a few header bytes. Typed messages are serialized straight into pooled buffers.

## Quick start

A server answers requests with an `ISocketPeerHandler`. Only `HandleRequestAsync` is required. Each request carries a service and an operation name to route on.

```csharp
using System.Net;
using PureGate.Socket.Fabric;
using PureGate.Socket.Handling;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

// Server
await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "greeter" });
await listener.StartAsync(new IPEndPoint(IPAddress.Any, 5000), new GreeterHandler());

// Client: connects on first use and reconnects when the connection drops
await using var client = new SocketClient("localhost", 5000, handler: null, new SocketFabricOptions { NodeId = "web-1" });
var reply = await client.RequestAsync<Greeting, Reply>("greeter", "hello", new Greeting("Ada"));
Console.WriteLine(reply.Message); // Hello, Ada!

sealed record Greeting(string Name);
sealed record Reply(string Message);

sealed class GreeterHandler : ISocketPeerHandler
{
  public ValueTask<SocketResponse> HandleRequestAsync(SocketPeer peer, SocketRequest request, CancellationToken cancellationToken)
  {
    var greeting = request.ReadPayload<Greeting>(peer.Codec);
    return ValueTask.FromResult(SocketResponse.Success(peer.Codec, new Reply($"Hello, {greeting.Name}!")));
  }
}
```

Messages are JSON by default. Use `RawSocketMessageCodec` to send bytes as they are, or implement your own `ISocketMessageCodec`.

## Guide

### Events

Events are one-way. By default, events from one connection are handled in the order they were sent (`EventOrdering`).

```csharp
await client.SendEventAsync("orders", "created", new OrderCreated(42));
```

```csharp
// In the handler
public ValueTask HandleEventAsync(SocketPeer peer, SocketEvent socketEvent, CancellationToken cancellationToken)
{
  var created = socketEvent.ReadPayload<OrderCreated>(peer.Codec);
  return ValueTask.CompletedTask;
}
```

### Streams

A stream request returns a `Stream` that the caller reads at its own pace. The producer only sends what the reader has room for.

```csharp
// In the handler
public ValueTask<SocketStreamResponse> HandleStreamAsync(SocketPeer peer, SocketStreamRequest request, CancellationToken cancellationToken)
{
  var name = Path.GetFileName(request.ReadPayload<string>(peer.Codec));
  return ValueTask.FromResult(SocketStreamResponse.Success(File.OpenRead(Path.Combine("exports", name))));
}
```

```csharp
await using var download = await client.OpenStreamAsync("files", "export", "orders.csv");
await using var file = File.Create("orders.csv");
await download.CopyToAsync(file);
```

### Batches

Send many requests in one round trip, each with its own result.

```csharp
var results = await peer.RequestBatchAsync(
[
  new SocketBatchItem(1, "orders", "get", peer.Codec.Serialize(new OrderId(1))),
  new SocketBatchItem(2, "orders", "get", peer.Codec.Serialize(new OrderId(2)))
], TimeSpan.FromSeconds(5));
```

### Calling clients from the server

Every connection is a `SocketPeer`, and both of its ends can send. A `SocketPeerRegistry` is an observer that keeps track of the connected peers, so a server can reach a node by its ID or broadcast to all of them.

```csharp
var registry = new SocketPeerRegistry();
await using var hub = new SocketListener(new SocketFabricOptions { NodeId = "hub", Observer = registry });
// ...
var status = await registry.RequestAsync<StatusQuery, Status>("web-1", "health", "status", new StatusQuery());
```

### Timeouts, retries and idempotency

Every call has a timeout (`RequestTimeout`, 10 seconds by default), and the deadline travels with the request: the remote handler's cancellation token fires when it passes. Cancelling a call cancels the remote handler too.

`SocketClient` can retry. Mark a call idempotent and every attempt carries the same idempotency key, so the server runs it once and answers repeats from its cache:

```csharp
var id = await client.RequestAsync<CreateOrder, OrderId>(
    "orders", "create", order,
    new SocketClientRequestOptions { Idempotent = true, MaxAttempts = 3 });
```

When a call times out, the client first checks whether the connection is still alive. A half-open connection is closed, so the retry goes out on a new one instead of timing out again.

### Security

Authentication runs in the handshake, and nothing else is accepted before it succeeds.

```csharp
var options = new SocketFabricOptions
{
  NodeId = "api-1",

  // Each pair of nodes shares its own secret, so a node can only authenticate as itself.
  Authenticator = new HmacSocketAuthenticator(remoteNodeId => secrets.For(remoteNodeId)),

  // TLS, with each node's ID bound to its certificate.
  StreamFilter = TlsSocketStreamFilter.Create(certificate, "api-1", requireClientCertificate: true, bindNodeIdToCertificate: true),

  // Per-request, per-stream and per-event checks.
  Authorizer = new MyAuthorizer()
};
```

`new HmacSocketAuthenticator("shared-secret")` uses one secret for the whole cluster. Before a peer authenticates, handshake frames are limited to 16 KB, and a listener caps concurrent handshakes (`MaxPendingHandshakes`) and connections (`MaxConnections`).

### Metrics

Everything is published under the meter `PureGate.Socket`:

```csharp
builder.Services.AddOpenTelemetry().WithMetrics(metrics => metrics.AddMeter(SocketMetrics.MeterName));
```

You can also watch them with `dotnet-counters monitor --counters PureGate.Socket`. Nothing is measured until a listener subscribes. For hooks into connection and request events, implement `ISocketObserver`.

### Serialization and AOT

`JsonSocketMessageCodec` uses System.Text.Json. For native AOT and trimming, give it a source-generated context:

```csharp
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(Greeting))]
[JsonSerializable(typeof(Reply))]
partial class AppJson : JsonSerializerContext;

var options = new SocketFabricOptions { Codec = new JsonSocketMessageCodec(AppJson.Default) };
```

## Limits and defaults

Every limit is an option on `SocketFabricOptions`.

| Option | Default | What it bounds |
|---|---|---|
| `RequestTimeout` | 10 s | Each call, unless the call passes its own timeout |
| `MaxFrameSizeBytes` | 1 MB | One frame; larger messages are split into chunks automatically |
| `MaxDecompressedPayloadSizeBytes` | 128 MB | One message, after reassembly and decompression |
| `MaxConnectionMemoryBytes` | 256 MB | Everything a connection holds for its remote |
| `MaxInFlightRequests` | 1,024 | This side's outstanding calls and streams on a connection |
| `MaxConcurrentHandlers` | 16 | Handlers running at once on a connection |
| `MaxQueuedHandlers` | 4,096 | Messages waiting for a handler, which is also how many the remote may send ahead |
| `StreamWindowBytes` | 1 MB | Unread data per stream, growing up to `MaxStreamWindowBytes` (64 MB) |
| `HeartbeatInterval` / `IdleTimeout` | 15 s / 45 s | Detection of dead connections |
| `MaxPendingHandshakes` / `MaxConnections` | 256 / unlimited | What a listener admits |

## Documentation

- [Library guide](https://github.com/PureGate/PureGate.Socket/blob/main/src/PureGate.Socket/README.md): flow control, the memory budget, serialization and the full list of metrics.
- [Wire protocol](https://github.com/PureGate/PureGate.Socket/blob/main/docs/PROTOCOL.md): the frame format, the handshake, and flow control on the wire.
- [Roadmap](https://github.com/PureGate/PureGate.Socket/blob/main/docs/ROADMAP.md) and [releasing](https://github.com/PureGate/PureGate.Socket/blob/main/docs/RELEASING.md).
- The [tests](https://github.com/PureGate/PureGate.Socket/tree/main/src/PureGate.Socket.Test) are working examples of every feature.

## Building and testing

```
dotnet build PureGate.Socket.slnx
dotnet test src/PureGate.Socket.Test
```

The suite includes seeded fuzz tests of the decoder and chaos tests through a proxy that fragments, delays and resets connections. A soak test, excluded by default, runs mixed chaotic traffic and then checks that every connection's state and every pooled buffer drains:

```
dotnet test src/PureGate.Socket.Test -c Release --filter Category=Soak
```

It runs for `PUREGATE_SOAK_SECONDS` (20 by default). The manual `soak` workflow on GitHub runs it for an hour.

When contributing: the build treats warnings as errors, and the public API is recorded in `src/PureGate.Socket/PublicAPI/`. A change to the public surface fails the build until `PublicAPI.Unshipped.txt` is updated, so API changes are visible in review. CI also requires 85% line coverage.

## License

[MIT](https://github.com/PureGate/PureGate.Socket/blob/main/LICENSE)
