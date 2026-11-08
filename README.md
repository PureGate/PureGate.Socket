# PureGate.Socket

[![NuGet](https://img.shields.io/nuget/v/PureGate.Socket.svg)](https://www.nuget.org/packages/PureGate.Socket)
[![CI](https://github.com/PureGate/PureGate.Socket/actions/workflows/ci.yml/badge.svg)](https://github.com/PureGate/PureGate.Socket/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/PureGate/PureGate.Socket/blob/main/LICENSE)

A fast, dependency-free messaging layer for long-lived TCP connections between .NET services.

PureGate.Socket gives two processes one connection that both of them can use: either side can call the other, send it events, or stream data to it. Every message is framed, multiplexed and flow-controlled, so many concurrent calls share one socket without blocking each other or overrunning the receiver. There's no HTTP, no code generation and no runtime dependency beyond .NET itself.

```
dotnet add package PureGate.Socket
```

## Features

Everything two .NET services need to talk over one long-lived connection, in a single package with no dependencies.

**Messaging**
- Request/response, typed or raw bytes
- Fire-and-forget events, handled in order per connection
- Pull-based binary streams of any size
- Batches: many requests or events in one round trip
- Both ends are peers: a server calls its clients as easily as they call it
- Large messages chunked automatically, with optional Brotli compression

**Under load**
- Credit-based flow control: a busy receiver slows its senders instead of being overrun
- A window per stream that grows to fit the link, so a slow reader holds back only its own stream
- A memory budget per connection: a message that doesn't fit is refused, not buffered
- Bounded handler concurrency and queues

**When things fail**
- Deadlines travel with the request, and cancelling a call cancels the remote handler
- Half-open connections detected in seconds
- A client that reconnects on its own, by address or host name
- Idempotent retries that the server runs once

**Security**
- Mutual HMAC authentication with a secret per pair of nodes
- TLS and mutual TLS, with node IDs bound to certificates
- Authorization per request, stream and event
- Limits on handshakes and connections before a peer authenticates

**Operations**
- OpenTelemetry-ready metrics and observer hooks
- A peer registry to reach a node by ID or broadcast to all
- Pluggable serialization: JSON (source-generated for AOT), raw bytes, or your own codec, such as MessagePack
- Fuzz, chaos and soak tested

**Speed**
- 77% of a bare socket's call rate, and over half a million calls a second on one shared connection
- Faster than gRPC, SignalR and StreamJsonRpc in every column of the [benchmark](#performance), with the fewest allocations

**Compared with the alternatives**

|                                | PureGate.Socket        | gRPC                               | SignalR                  | StreamJsonRpc                |
|--------------------------------|------------------------|------------------------------------|--------------------------|------------------------------|
| Either side can call the other | ✓                      | Through streams only               | ✓                        | ✓                            |
| Hosting                        | Any process, plain TCP | ASP.NET Core, HTTP/2               | ASP.NET Core, WebSockets | Any stream                   |
| Contract                       | Plain C# types         | `.proto` files and code generation | Hub methods              | Interfaces or method names   |
| Runtime dependencies           | None                   | gRPC, Protobuf, ASP.NET Core       | ASP.NET Core             | Several packages             |

## Performance

Echoing a 1 KB payload with libraries' recommended fast setup:

| Library                     | Sequential calls/s |       p50 |       p99 | 64 callers calls/s | Alloc per call |
|-----------------------------|-------------------:|----------:|----------:|-------------------:|---------------:|
| Raw socket (ceiling)        |             47,558 |     20 µs |     36 µs |                n/a |          128 B |
| **PureGate.Socket**         |         **36,668** | **26 µs** | **39 µs** |        **548,710** |    **3,253 B** |
| gRPC (HTTP/2)               |              6,063 |    161 µs |    236 µs |            120,166 |       10,345 B |
| SignalR (MessagePack)       |             24,335 |     37 µs |     65 µs |            329,210 |        4,422 B |
| StreamJsonRpc (MessagePack) |             27,067 |     32 µs |     50 µs |            279,519 |        5,162 B |

- **Lowest latency:** 77% of a bare socket's call rate, with the best p50 and p99 of the four libraries. That's 1.35× StreamJsonRpc, 1.5× SignalR and 6× gRPC.
- **Highest throughput on one connection:** with 64 callers sharing a single connection, 1.7× SignalR, 2× StreamJsonRpc and 4.6× gRPC.
- **Least memory:** the fewest bytes allocated per call, counting both client and server.

Measured on Windows, 20 cores, .NET 10, over loopback with client and server in one process. "Sequential" makes one call at a time; "64 callers" share one connection. Numbers vary by machine; the [benchmark](https://github.com/PureGate/PureGate.Socket/tree/main/benchmark/Competitors) and its methodology are in the repository, so you can run it yourself.

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

### Handlers that call back the peer they serve

Each connection runs at most `MaxConcurrentHandlers` handlers at once, and a handler holds its slot until it returns. That allows a deadlock: node A's handler for a request from B calls B and waits for the answer, while B's handlers do the same to A. Once every handler on both sides is waiting, each callback is queued behind the handlers waiting for it, and every call runs into its timeout. A higher limit only moves the point where this happens.

Give the callback operations a handler lane, a pool of their own, on both nodes:

```csharp
var options = new SocketFabricOptions
{
    HandlerLanes = [new SocketHandlerLane("domain", "sync_columns", MaxConcurrentHandlers: 8)]
};
```

Requests and stream requests for a lane's operation, or for every operation of its service when `Operation` is null, run on the lane's own workers. They never wait behind the general handlers, and the general handlers never wait behind them. The sending side also keeps part of its message credit for lane requests, so general messages piling up at a busy node cannot stop a lane request from being sent. Events always run on the general handlers.

### Shutting down

`DisposeAsync` closes a connection without waiting for the handlers still running on it: they see their cancellation tokens cancelled and finish on their own, so a handler can even dispose its own connection. When you are about to release something the handlers use, wait for them first:

```csharp
await peer.DisposeAsync();
await peer.WaitForHandlersAsync(); // every handler callback has returned, OnDisconnectedAsync included
```

Don't await `WaitForHandlersAsync` from one of the connection's own handlers: it would wait for that handler.

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

For the fastest typed calls, use a binary format such as MessagePack. Add the `MessagePack` package to your project and copy this codec; PureGate.Socket itself stays dependency-free:

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

var options = new SocketFabricOptions { Codec = MessagePackSocketMessageCodec.Shared };
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
| `MaxConcurrentHandlers` | 100 | Handlers running at once on a connection |
| `HandlerLanes` | none | Operations with a handler pool of their own, each with its own limit (16 by default) |
| `MaxQueuedHandlers` | 4,096 | Messages waiting for a handler, which is also how many the remote may send ahead |
| `StreamWindowBytes` | 1 MB | Unread data per stream, growing up to `MaxStreamWindowBytes` (64 MB) |
| `HeartbeatInterval` / `IdleTimeout` | 15 s / 45 s | Detection of dead connections |
| `MaxPendingHandshakes` / `MaxConnections` | 256 / unlimited | What a listener admits |
| `SocketBufferBytes` | 1 MB on Windows, system default elsewhere | Kernel send and receive buffers per connection |

## Documentation

- [Library guide](https://github.com/PureGate/PureGate.Socket/blob/main/src/PureGate.Socket/README.md): flow control, the memory budget, serialization and the full list of metrics.
- [Wire protocol](https://github.com/PureGate/PureGate.Socket/blob/main/docs/PROTOCOL.md): the frame format, the handshake, and flow control on the wire.
- [Competitor benchmark](https://github.com/PureGate/PureGate.Socket/tree/main/benchmark/Competitors): PureGate.Socket against gRPC, SignalR and StreamJsonRpc, with results.
- [Roadmap](https://github.com/PureGate/PureGate.Socket/blob/main/docs/ROADMAP.md).
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

When contributing: the build treats warnings as errors, and CI requires 85% line coverage.

## License

[MIT](https://github.com/PureGate/PureGate.Socket/blob/main/LICENSE)
