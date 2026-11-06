# PureGate.Socket plan: correctness first, then speed

**Assumptions.** The repo has a single commit, so I've assumed no peers are deployed yet and the plan breaks the wire format and the API wherever that helps. The library itself stays dependency-free; tests and benchmarks may use dev-only tools.

**On "bug free":** no plan can promise that. What it can do is make each known bug impossible by design where possible, and catch the rest with fuzzing, chaos and soak tests.

The order is deliberate. The biggest speed gains come from reworking the connection's core, and that same rework structurally fixes the worst bugs. Measurement comes first so every change is judged against numbers.

---

## Phase 0 — Safety net (before changing any code)

1. **Turn the 10 confirmed bugs into failing regression tests.** Most can be lifted almost directly from the probe program. Add stress tests for the 5 suspected bugs: the pooling race, the stream-buffer race, event ordering, a read/write deadlock between peers, and cancelling a write mid-frame.
2. **Extend the benchmark:**
   - p50/p99/p99.9 latency, not just throughput.
   - Payload sizes of 64 B, 1 KB, 64 KB and 1 MB.
   - Scaling across 1, 8 and 64 connections, with TLS and compression each on and off.
   - A **raw socket ping-pong baseline**, so targets are measured against what the machine can actually do.
   - Save the current results as a JSON baseline in the repo.
3. **CI:** Windows and Linux, with the test suite repeated about 20 times per run to catch flaky races. Treat warnings as errors. (Nullable annotations moved to Phase 5; see Status.)

**Done when:** about 15 new tests fail for the right reasons and the baseline is committed.

## Phase 1 — Local bug fixes (small, low-risk changes)

| Bug | Fix |
|---|---|
| #1 a handler's `OperationCanceledException` kills a worker | Catch every exception per item. The worker loop can never exit while the connection is open. Send a `handler_failed` response. |
| #2 compression drops `OperationId` | Apply compression as a flag at encode time, instead of rebuilding the frame (which silently drops fields). |
| #3 handshake reader swallows frames | Give `SocketPeer` the same reader and writer that the handshake used. |
| #4 an event handler error closes the connection | Report it to the observer and keep going. Only an authorization denial closes the connection, and that is configurable. |
| #5 stream lifetime capped by the open timeout | The deadline covers only the stream's start. Add a separate idle timeout for an open stream. |
| #7 / #8 unchecked counts allocate huge arrays | Check every declared count against the bytes actually present. Require `chunkCount == ceil(total / chunkSize)`. Add a per-connection budget for bytes held in partial chunked messages. |
| #10 a retry closes the shared connection | Retry on the same connection after a timeout. Close it only on `connection_closed`. |
| Typed client request reconnects after success | Take the codec from `Options` instead of reconnecting. |
| Error codes | Add a retryable `overloaded` code, used instead of `protocol_error` for local overload. |
| Leaked exception text | Add an `IncludeExceptionDetails` option, off by default. |

**Done when:** all the regression tests pass except #6 and #9, which need Phase 2.

## Phase 2 — Rework the connection core (biggest speed and correctness lever)

1. **A single writer loop.**
   - Everything sent goes into one queue. One writer task encodes frames straight into the pipe and flushes only when the queue is empty. Today there is one system call per frame, so batching flushes this way is the main throughput gain.
   - This also removes the contended write lock.
   - Cancellation only removes a frame that hasn't been sent yet, so a half-written frame becomes impossible.
2. **Priority lanes in that queue.** Control frames (pong, cancel, close, credits) go first, then responses, then requests and events, then stream data and chunks. Large messages are interleaved chunk by chunk, so they no longer block everything else.
3. **The read loop never waits on the socket or on a consumer.** It only decodes, dispatches and queues. This removes the risk of two peers deadlocking while each waits to write.
4. **Credit-based flow control**, like HTTP/2's window updates, per stream and per connection.
   - A slow stream consumer stalls only its own stream (fixes #6).
   - Event senders wait for credit instead of the receiver closing the connection (fixes #9).
5. **A pending-request table with generations.** Requests live in a fixed array of slots, and the message ID includes a generation counter. This replaces the `ConcurrentDictionary` lookup on every response. (The pooling race itself was already closed in Phase 1.)
6. **A timeout wheel.** One timer per connection replaces a cancellation source and a timer per request, on both the client and server sides.
7. **Ordered events.** By default, events from one connection are processed in order, with an option to process in parallel by key. Requests stay concurrent.
8. ~~**Pull stream fix.**~~ Done in Phase 1, after the stress test showed real data corruption.
9. **Fewer thread hops.** An opt-in way to run a short handler directly on the read loop, and a configurable way to complete client requests without an extra hop.
10. **A raw socket transport.** When there's no TLS, read and write the socket directly (the approach Kestrel uses) instead of going through `NetworkStream`. Keep the stream path for TLS. Adopt this only if the benchmark proves the gain.

**Done when:** all regression tests pass, the 20× repeated runs are clean, and the benchmark shows clear wins on concurrent load.

## Phase 3 — Wire protocol v2

- **Negotiate the version once** in the handshake, instead of sending a 4-byte version in every frame.
- **Compact frame headers.** Length, kind, flags and a variable-length ID, plus only the fields that kind of frame needs. That's about 8–12 bytes, versus about 67 bytes plus strings today.
- **Method table.** The first call sends the `service/operation` name and assigns it a number; later calls send only the number. This **merges the normal and fast request paths** and removes the duplicated code.
- **Optional fields:** the idempotency key and error message are sent only when present, signalled by a flag. Error codes become small numbers.
- **Compression:**
  - Use span-based Brotli with pooled buffers, with no `MemoryStream`.
  - Compress each chunk, so large messages benefit too.
  - Decompress within a memory budget, so it doesn't block the read loop.
  - Benchmark Deflate as a cheaper alternative.
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
- **Gates:** at least 85% line coverage and every decoder branch covered. Add a public-API analyzer and complete the nullable annotations.

## Phase 6 — Release

Publish the protocol spec, migration notes, a versioned NuGet package and a performance report comparing the new numbers with the baseline.

---

## Performance targets

Targets are stated against the raw-socket ceiling that Phase 0 measures on the same machine, because absolute numbers change from machine to machine. Baseline: [benchmarks/baseline-phase0.json](benchmarks/baseline-phase0.json) (Windows, 20 cores, .NET 10, loopback).

The first draft of this table set 60k/s sequential and 500k/s on 8 connections. Phase 0 showed both are above what raw sockets reach in the same scenario on this machine (about 50k/s and 203k/s), so they were restated. The 500k/s goal moves to a pipelined scenario (many requests in flight per connection), which Phase 2 adds to the benchmark.

| Metric | Raw socket ceiling | PureGate at Phase 0 | Target |
|---|---|---|---|
| Sequential RPC, 1 KB | 50k/s, p50 20 µs | 25k/s (51%), p50 36 µs | ≥ 80% of ceiling |
| Fast RPC, 1 KB | 50k/s | 28k/s, barely faster than normal RPC | Merged into one path, ≥ 85% of ceiling |
| 8 connections, 1 KB | 203k/s | 94k/s (46%), p99 388 µs | ≥ 80% of ceiling, p99 within 2× of ceiling |
| 64 connections, 1 KB | 293k/s | 147k/s (50%), p99 3.1 ms | ≥ 80% of ceiling, p99 within 2× of ceiling |
| Pipelined, 1 connection | to be measured in Phase 2 | to be measured | ≥ 500k/s |
| Allocations per RPC, 1 KB | ~390 B | ~7.6 KB | < 500 B |
| Stream throughput, plain / TLS | — | 2.2 GB/s / 0.9 GB/s | ≥ 3 GB/s / ≥ 1.2 GB/s |
| Brotli on 64 KB responses | — | **broken** (bug #15) | Works; benchmarked |

Run-to-run noise on this machine is large: the 8-connection PureGate result was 144k/s in one run and 94k/s in the next. Compare medians of several runs before claiming a gain.

## Where each bug gets fixed

| Phase | Bugs fixed |
|---|---|
| Phase 1 (done) | #1, #2, #3, #4, #5, #7, #8, #10, #11, #14, #15, #16, #17, #18, the stream-buffer race, the pooling race, cancelling a write mid-frame, the client reconnect, leaked exception text |
| Phase 2 | #6, #9, #12 event ordering, #13 the read/write deadlock |
| Phase 5 | Identity binding, resource caps, and whatever the fuzzing and chaos tests find |

## Status

### Phase 0 — done

- **Regression tests:** [SocketFabricTests.Regressions.cs](../src/PureGate.Socket.Test/SocketFabricTests.Regressions.cs). On the unfixed code, 16 regression tests failed for the expected reasons, 3 stress tests passed (races too narrow to hit) and the bidirectional flood hung.
- **Benchmark:** `--suite matrix` adds a raw-socket ceiling, p50/p99/p99.9 latency, payload sizes from 64 B to 1 MB, 1/8/64 connections, TLS and Brotli variants, and `--json` output. A failing scenario is recorded, not fatal.
- **CI:** [.github/workflows/ci.yml](../.github/workflows/ci.yml) runs on Windows and Linux, repeats the suite 20 times and does a quick benchmark run.
- **Warnings as errors** via `Directory.Build.props`. Nullable annotations moved to Phase 5: enabling them now produces 140 warnings in APIs that Phases 2–4 rewrite anyway.

Phase 0 confirmed four defects beyond the original ten:

| # | Defect |
|---|---|
| 11 | Disposing a peer with requests in flight made those callers throw `ObjectDisposedException` instead of returning `connection_closed`. |
| 12 | Events from one peer are handled out of order (suspected before, now confirmed). |
| 13 | Two peers flooding each other deadlock (suspected before, now confirmed). |
| 14 | `DisposeAsync` hangs forever when the remote stops reading, because the close frame waits on the write lock with no timeout. |
| 15 | Brotli corrupted every response and stream chunk of 16 KB or more: the frame was flagged compressed but carried the uncompressed bytes. |

### Phase 1 — done

All Phase 1 items are in, plus #11, #14 and #15. Two more were found and fixed along the way:

- **Stream-buffer race (moved up from Phase 2).** It was confirmed by the stress test as real data corruption: when a connection dropped, the close path returned the consumer's current pooled buffer to the pool while the consumer was still copying from it. Only the consumer thread now touches its buffers, and data received before a remote error is delivered before the error.
- **#16: disposing a stream early killed the connection.** Chunks still in flight hit the closed buffer and faulted the read loop.
- **#17: cancellation leaked between unrelated writes.** Each write passed its caller's token into the connection's shared `PipeWriter`. A token that fired around the end of one flush left the writer cancelled, and the next write, from a different caller, failed with an `OperationCanceledException` that no handler expected. It showed up as flaky failures in two tests. Callers' tokens now only cancel waiting for the write lock; a frame that has started is always written whole. This also closes "cancelling a write mid-frame", which was planned for Phase 2.
- **#18: a late Cancel frame could close the connection or cancel the wrong request.** Server-side handler cancellation sources were pooled. When a Cancel arrived just as its request finished, the read loop called `Cancel()` on a source that had been disposed (an `ObjectDisposedException` that closed the connection, about 1 run in 35 of the stress test) or reused by another request (cancelling that request). Server-side sources are no longer pooled or disposed. The stress test then passed 80 runs out of 80.
- **Pooling race (moved up from Phase 2).** Never observed. The stress failures first blamed on it turned out to be #17. It is fixed anyway because the fix is small: each pooled request object is armed with its message ID, and completion atomically swaps that exact ID out, so a late response can't complete a reused object.

New options: `StreamIdleTimeout`, `IncludeExceptionDetails`, `CloseOnUnauthorizedEvent`, `MaxChunkedBytesInProgress`. New observer hook: `ISocketObserver.HandlerFailed`. New error code: `overloaded`.

Behavior changes callers may notice:

- Handler exception messages are no longer sent to the remote peer unless `IncludeExceptionDetails` is set. A `SocketProtocolException` thrown by a handler keeps its own code and message.
- Local overload (in-flight limit, full handler queue) reports `overloaded` instead of `protocol_error`. `SocketClient` treats it as retryable.
- An event handler that throws no longer closes the connection.
- A stream's open timeout no longer limits how long the stream runs; `StreamIdleTimeout` limits the gap between chunks.
- Chunked messages must arrive in order with consistent sizes, which is what the library always sent.