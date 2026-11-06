# PureGate.Socket plan: correctness first, then speed

**Assumptions.** No peers are deployed yet, so the plan breaks the wire format and the API wherever that helps. The library itself stays dependency-free; tests and benchmarks may use dev-only tools.

**On "bug free":** no plan can promise that. What it can do is make each known bug impossible by design where possible, and catch the rest with fuzzing, chaos and soak tests.

The order is deliberate. The biggest speed gains come from reworking the connection's core, and that same rework structurally fixes the worst remaining bugs. Every change is judged against the benchmark baseline.

Phases 0 (safety net) and 1 (local bug fixes) are done, and so is flow control from Phase 2.

---

## Phase 2 — Rework the connection core (biggest speed and correctness lever)

1. **A single writer loop.**
   - Everything sent goes into one queue and one writer task encodes it into the pipe. Flushes are already coalesced under the write lock, so the remaining gains are no lock contention and the priority lanes below.
2. **Priority lanes in that queue.** Control frames (pong, cancel, close, credits) go first, then responses, then requests and events, then stream data and chunks. Large messages are interleaved chunk by chunk, so they no longer block everything else.
3. **A pending-request table with generations.** Requests live in a fixed array of slots, and the message ID includes a generation counter. This replaces the `ConcurrentDictionary` lookup on every response.
4. **A timeout wheel.** One timer per connection replaces a cancellation source and a timer per request, on both the client and server sides.
5. **Ordered events.** By default, events from one connection are processed in order, with an option to process in parallel by key. Requests stay concurrent.
6. **Fewer thread hops.** An opt-in way to run a short handler directly on the read loop, and a configurable way to complete client requests without an extra hop.
7. **A raw socket transport.** When there's no TLS, read and write the socket directly (the approach Kestrel uses) instead of going through `NetworkStream`. Keep the stream path for TLS. Adopt this only if the benchmark proves the gain.
8. **On-demand liveness check.** When a request times out, send a ping with a short deadline. If no pong arrives, close the connection, so `SocketClient` retries on a fresh one instead of a half-open link.
9. **Benchmark: repeated runs.** Run each scenario several times and report the median and spread.

**Done when:** the ordered-events test passes, CI's 20× repeated runs are clean, and the benchmark shows clear wins on concurrent load.

## Phase 3 — Wire protocol v2

- **Negotiate the version once** in the handshake, instead of sending a 4-byte version in every frame.
- **Compact frame headers.** Length, kind, flags and a variable-length ID, plus only the fields that kind of frame needs. That's about 8–12 bytes, versus about 67 bytes plus strings today.
- **Method table.** The first call sends the `service/operation` name and assigns it a number; later calls send only the number. This **merges the normal and fast request paths** and removes the duplicated code.
- **Optional fields:** the idempotency key and error message are sent only when present, signalled by a flag. Error codes become small numbers.
- **Compression:**
  - Use span-based Brotli with pooled buffers, with no `MemoryStream`.
  - Compress each chunk, so large messages benefit too.
  - Decompress within a memory budget, so it doesn't block the read loop.
  - Benchmark Deflate as a cheaper alternative. Zstandard isn't in the .NET 10 base library, so it would need a third-party package; benchmark it before deciding whether that's worth a dependency.
- **Handshake binding:** derive a session key from the handshake nonces (HKDF). Optionally sign every frame when TLS isn't used.
- Write the format down in a `PROTOCOL.md` spec.

## Phase 4 — Zero-copy, low-allocation APIs

- **Codec API:** `Serialize<T>(T, IBufferWriter<byte>)` and `Deserialize<T>(ReadOnlySequence<byte>)`. JSON is written straight into the pipe, with support for source-generated serializers (AOT and trimming friendly).
- **No intermediate `byte[]`** for outgoing payloads. Response payloads can optionally come from a pooled buffer the caller must dispose; copying stays the safe default.
- **No `SocketFrame` object on the hot path.** Use struct headers instead.
- **Interned method names**, so the server doesn't allocate a string per request.
- **Batch encoders** write directly to the pipe.

## Phase 5 — Hardening

- **Fuzzing:** run SharpFuzz on the frame decoder, batch codecs, chunk reassembly and handshake. Add randomized round-trip tests inside NUnit.
- **Chaos tests** through an in-process proxy: 1-byte fragments, added latency, slow readers, connections reset mid-frame, reconnect storms, duplicate connections.
- **Nightly one-hour soak test** with leak checks: pending requests, rented pool buffers, credits and queue depths must all return to zero.
- **Systematic concurrency testing** with Microsoft Coyote on the request and stream lifecycles (optional).
- **Resource limits:**
  - A per-connection memory budget.
  - A cap on the listener's connections and pending handshakes.
  - A small frame-size limit before authentication, around 16 KB.
- **Security:**
  - With mTLS, require the node ID to match the certificate, so a node can't claim another's identity.
  - Scope the idempotency cache by the authenticated identity.
  - Support per-node secrets.
- **Metrics** through the built-in `System.Diagnostics.Metrics`.
- **Gates:** at least 85% line coverage and every decoder branch covered. Add a public-API analyzer and turn on nullable annotations (deferred until Phases 2–4 have reshaped the public API).

## Phase 6 — Release

Publish the protocol spec, migration notes, a versioned NuGet package and a performance report comparing the new numbers with the baseline.

---

## Performance targets

Targets are stated against the raw-socket ceiling measured on the same machine, because absolute numbers change from machine to machine. Baseline: [benchmarks/baseline-phase0.json](benchmarks/baseline-phase0.json) (Windows, 20 cores, .NET 10, loopback).

| Metric | Raw socket ceiling | PureGate baseline | Target |
|---|---|---|---|
| Sequential RPC, 1 KB | 50k/s, p50 20 µs | 25k/s (51%), p50 36 µs | ≥ 80% of ceiling |
| Fast RPC, 1 KB | 50k/s | 28k/s, barely faster than normal RPC | Merged into one path, ≥ 85% of ceiling |
| 8 connections, 1 KB | 203k/s | 94k/s (46%), p99 388 µs | ≥ 80% of ceiling, p99 within 2× of ceiling |
| 64 connections, 1 KB | 293k/s | 147k/s (50%), p99 3.1 ms | ≥ 80% of ceiling, p99 within 2× of ceiling |
| Pipelined, 1 connection | to be measured | to be measured | ≥ 500k/s |
| Allocations per RPC, 1 KB | ~390 B | ~7.6 KB | < 500 B |
| Stream throughput, plain / TLS | — | 2.2 GB/s / 0.9 GB/s | ≥ 3 GB/s / ≥ 1.2 GB/s |

Run-to-run noise on this machine is large: the 8-connection PureGate result was 144k/s in one run and 94k/s in the next. Compare medians of several runs before claiming a gain.
