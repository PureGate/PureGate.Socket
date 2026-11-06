# PureGate.Socket

A lightweight, dependency-free TCP messaging fabric for .NET. It sits on top of plain sockets and gives you framed, multiplexed request/response calls, events, batches and streams, with pluggable security, serialization and observability.

Targets **.NET 10**.

## Features

- **Request/response**: typed (`RequestAsync<TRequest, TResponse>`) or raw calls, with per-request options.
- **Events**: fire-and-forget messages, singly or in batches.
- **Streams**: pull-style binary streams opened from a client request.
- **Batching**: send many requests or events in a single round trip.
- **Flow control**: senders wait when the remote is busy instead of overrunning it; stream windows size themselves to the link.
- **Ordered events**: events from one connection are handled in order by default.
- **Idempotency**: a built-in cache so retried requests are not executed twice.
- **Compact wire protocol**: a request costs a few header bytes after its method's first use; see [docs/PROTOCOL.md](docs/PROTOCOL.md).
- **Chunking and compression**: large payloads are split into frames and can be compressed.
- **Memory budget**: each connection caps what it holds for its remote, and refuses what does not fit instead of running out of memory.
- **Security**: pluggable authentication and authorization (`ISocketAuthenticator`, `ISocketAuthorizer`), with an HMAC authenticator that supports per-node secrets. TLS and mTLS are available through `TlsSocketStreamFilter`, which can bind node IDs to certificates. Handshake frame size, pending handshakes and connection counts are limited before a peer authenticates.
- **Serialization**: JSON codec by default (source-generated contexts supported, for AOT and trimming), raw codec for byte payloads, or implement `ISocketMessageCodec`. Typed messages are serialized straight into pooled buffers.
- **Metrics**: request latency, connections, bytes, queues, memory and flow control through `System.Diagnostics.Metrics` (meter `PureGate.Socket`), ready for OpenTelemetry.
- **Observability**: hook in with `ISocketObserver`; combine several with `CompositeSocketObserver`.
- **Client fabric**: `SocketClient` with a peer registry and peer selection policies.

## Layout

```
src/
  PureGate.Socket/             Library
    Fabric/                    SocketClient, peer registry, selection policies
    Framing/                   Frame reader/writer and flags
    Handling/                  ISocketPeerHandler
    Observability/             ISocketObserver and helpers
    Protocol/                  Messages, options, errors, idempotency cache
    Security/                  Authenticators and authorizers
    Serialization/             Message codecs
    Transport/                 Listener, connector, SocketPeer, TLS filter
  PureGate.Socket.Test/        NUnit tests
  PureGate.Socket.Benchmarks/  Benchmarks
```

## Install

```
dotnet add package PureGate.Socket
```

## Quick start

Implement `ISocketPeerHandler` to serve requests. Only `HandleRequestAsync` is required, and the other members have default implementations.

```csharp
await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new MyHandler());

var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
await using var client = await connector.ConnectAsync(listener.EndPoint, new MyHandler());

var reply = await client.RequestAsync<EchoRequest, EchoRequest>("service", "echo", new EchoRequest("hello"));
```

See [SocketFabricTests.cs](src/PureGate.Socket.Test/SocketFabricTests.cs) for working examples of requests, streams, batches, authentication and error handling.

## Build and test

```
dotnet build PureGate.Socket.slnx
dotnet test src/PureGate.Socket.Test
```

The library builds with warnings as errors. Its public API is recorded in [PublicAPI.Unshipped.txt](src/PureGate.Socket/PublicAPI/PublicAPI.Unshipped.txt) (moved to `PublicAPI.Shipped.txt` at each release): a change to the public surface fails the build until that file is updated, so every API change shows up in review. CI also fails when line coverage of the library drops below 85%.

Releases are published with the manual **Publish Nuget Package** workflow; see [docs/RELEASING.md](docs/RELEASING.md).

The suite includes seeded fuzz tests and chaos tests through a proxy that fragments, delays and resets connections. A soak test, excluded by default, runs mixed chaotic traffic and then checks that connection state and pooled buffers drain. It runs for `PUREGATE_SOAK_SECONDS` (20 by default):

```
dotnet test src/PureGate.Socket.Test -c Release --filter Category=Soak
```

On GitHub, run the `soak` workflow from the Actions tab. It soaks for an hour by default, on Ubuntu, Windows or both.

## License

PureGate.Socket is licensed under the **[MIT License](https://github.com/PureGate/PureGate.Socket/blob/main/LICENSE)**.
