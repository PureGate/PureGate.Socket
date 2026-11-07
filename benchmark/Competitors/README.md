# Competitor benchmark

Compares PureGate.Socket with the libraries .NET teams usually pick for service-to-service messaging:

- **gRPC** (`Grpc.AspNetCore`, HTTP/2, protobuf)
- **SignalR** (WebSockets, MessagePack protocol)
- **StreamJsonRpc** (MessagePack over a length-prefixed TCP stream)
- **A raw socket echo**, as the ceiling for any protocol on the machine

Every library echoes the same 1 KB payload, set up the way its documentation recommends for speed, with default options otherwise:

| Library         | Setup                                                          |
|-----------------|----------------------------------------------------------------|
| PureGate.Socket | Raw `byte[]` requests                                          |
| gRPC            | Unary call with a protobuf `bytes` message, HTTP/2 without TLS |
| SignalR         | Hub invocation, MessagePack protocol                           |
| StreamJsonRpc   | `MessagePackFormatter` with `LengthHeaderMessageHandler`       |

Two scenarios run for each:

- **Sequential:** calls one after another, measuring each call's latency (p50, p99).
- **Concurrent:** many callers calling in a loop, all sharing one connection.

Each scenario runs for a fixed time rather than a fixed number of calls, so every library gets the same time to have its code optimized by the JIT, however fast it is.

## Run

```
set DOTNET_TieredCompilation=0
dotnet run -c Release --project benchmark/Competitors
```

On Linux or macOS, use `DOTNET_TieredCompilation=0 dotnet run -c Release --project benchmark/Competitors`.

| Option                 | Default | Meaning                                      |
|------------------------|--------:|----------------------------------------------|
| `--duration <seconds>` |       1 | Measured time per scenario                   |
| `--warmup <seconds>`   |       0 | Unmeasured calls before each scenario        |
| `--callers <n>`        |      64 | Concurrent callers sharing the connection    |
| `--payload <bytes>`    |    1024 | Echoed payload size (gRPC allows up to 4 MB) |

With the defaults the whole run takes about 10 seconds. With no warmup, the first calls of each library include JIT compilation, the same for all of them. Run it three times and compare medians; single runs vary.

Two settings answer different questions:

- **Tiered compilation off, no warmup** (the default above): every method is fully optimized the first time it is compiled, so a short run measures optimized code.
- **Steady state, as a long-running server sees it:** leave tiered compilation on and give the JIT time to optimize, for example `dotnet run -c Release --project benchmark/Competitors -- --warmup 2 --duration 3`. Tiered compilation adds profile-guided optimization, which helps some libraries more than others, so this can rank them differently. Without the warmup, a short run with tiered compilation on mostly measures the JIT warming up.

This project isn't part of `PureGate.Socket.slnx`, so the library's build and CI don't pull in ASP.NET Core and gRPC.

## How to read the results

- Client and server run in one process over loopback. Allocations count both sides, including the two copies of the 1 KB payload.
- Loopback has no network latency. A real network adds the same latency to every library, which narrows the sequential gaps; throughput on a shared connection is less affected.
- SignalR runs one invocation at a time per connection by default, on the thread that read it; PureGate.Socket and StreamJsonRpc dispatch concurrently. With an echo handler that does no work, inline dispatch costs nothing; with handlers that wait on I/O, concurrent dispatch matters.

## Results

Windows, 20 cores, .NET 10, over loopback with client and server in one process:

| Library                     | Sequential calls/s |       p50 |       p99 | 64 callers calls/s | Alloc per call |
|-----------------------------|-------------------:|----------:|----------:|-------------------:|---------------:|
| Raw socket (ceiling)        |             47,558 |     20 µs |     36 µs |                n/a |          128 B |
| **PureGate.Socket**         |         **36,668** | **26 µs** | **39 µs** |        **548,710** |    **3,253 B** |
| gRPC (HTTP/2)               |              6,063 |    161 µs |    236 µs |            120,166 |       10,345 B |
| SignalR (MessagePack)       |             24,335 |     37 µs |     65 µs |            329,210 |        4,422 B |
| StreamJsonRpc (MessagePack) |             27,067 |     32 µs |     50 µs |            279,519 |        5,162 B |

Package versions: Grpc.AspNetCore 2.85.0, Microsoft.AspNetCore.SignalR.Client 10.0.14, StreamJsonRpc 2.26.150.
