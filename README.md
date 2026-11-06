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
- **Security**: pluggable authentication and authorization (`ISocketAuthenticator`, `ISocketAuthorizer`), with an HMAC authenticator included. TLS is available through `TlsSocketStreamFilter`.
- **Serialization**: JSON codec by default, raw codec for byte payloads, or implement `ISocketMessageCodec`.
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

## License

PureGate.Socket is licensed under the **[MIT License](https://github.com/PureGate.Socket/PureGate.Socket?tab=MIT-1-ov-file#readme)**.
