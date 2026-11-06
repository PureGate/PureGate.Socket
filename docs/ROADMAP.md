# PureGate.Socket plan: correctness first, then speed

Phases 0 (safety net) and 1 (local bug fixes) are done, and so are flow control, ordered events and the liveness check from Phase 2.

---

## Phase 2 — Rework the connection core (biggest speed and correctness lever)

1. **A single writer loop.**
   - Everything sent goes into one queue and one writer task encodes it into the pipe. Flushes are already coalesced under the write lock, so the remaining gains are no lock contention and the priority lanes below.
2. **Priority lanes in that queue.** Control frames (pong, cancel, close, credits) go first, then responses, then requests and events, then stream data and chunks. Large messages are interleaved chunk by chunk, so they no longer block everything else.
3. **A pending-request table with generations.** Requests live in a fixed array of slots, and the message ID includes a generation counter. This replaces the `ConcurrentDictionary` lookup on every response.
4. **A timeout wheel.** One timer per connection replaces a cancellation source and a timer per request, on both the client and server sides.
5. **Fewer thread hops.** An opt-in way to run a short handler directly on the read loop, and a configurable way to complete client requests without an extra hop.
6. **A raw socket transport.** When there's no TLS, read and write the socket directly (the approach Kestrel uses) instead of going through `NetworkStream`. Keep the stream path for TLS. Adopt this only if the benchmark proves the gain.
7. **Benchmark: repeated runs.** Run each scenario several times and report the median and spread.

**Done when:** CI's 20× repeated runs are clean and the benchmark shows clear wins on concurrent load.

## Phase 3 — Protocol follow-ups

The wire format is specified in [PROTOCOL.md](PROTOCOL.md). Still to do:

- **Compression:** benchmark Deflate as a cheaper alternative to Brotli, and compress with span-based Brotli and pooled buffers on the send side too (receiving already does). Zstandard isn't in the .NET 10 base library, so it would need a third-party package; benchmark it before deciding whether that's worth a dependency.
- **Handshake binding:** derive a session key from the handshake nonces (HKDF). Optionally sign every frame when TLS isn't used.

## Phase 4 — Zero-copy, low-allocation APIs

- **Opt-in pooled payloads** for raw calls: a response payload from a pooled buffer the caller must dispose, and request payloads valid only while the handler runs. Copying stays the default.
- **No `SocketFrame` object on the hot path.** Use struct headers instead.

## Phase 5 — Hardening

- **Coverage-guided fuzzing:** run SharpFuzz on the frame decoder, batch codecs, chunk reassembly and handshake, and add the crashes it finds to the seeded NUnit fuzz tests.
- **More chaos scenarios:** duplicate connections from one node ID, and peers that read very slowly at the socket level.
- **Systematic concurrency testing** with Microsoft Coyote on the request and stream lifecycles (optional).
- **Branch coverage:** cover every decoder branch (branch coverage is 69% today, against 86% of lines), then gate branch coverage in CI as line coverage is.

## Phase 6 — Performance report

A performance report comparing the numbers with the Phase 0 baseline and the raw-socket ceiling, from the repeated-run medians of Phase 2.

---

## Performance targets

Targets are stated against the raw-socket ceiling measured on the same machine, because absolute numbers change from machine to machine. Baseline: [benchmarks/baseline-phase0.json](benchmarks/baseline-phase0.json) (Windows, 20 cores, .NET 10, loopback).

| Metric | Raw socket ceiling | PureGate baseline | Target |
|---|---|---|---|
| Sequential RPC, 1 KB | 50k/s, p50 20 µs | 25k/s (51%), p50 36 µs | ≥ 80% of ceiling |

| 8 connections, 1 KB | 203k/s | 94k/s (46%), p99 388 µs | ≥ 80% of ceiling, p99 within 2× of ceiling |
| 64 connections, 1 KB | 293k/s | 147k/s (50%), p99 3.1 ms | ≥ 80% of ceiling, p99 within 2× of ceiling |
| Pipelined, 1 connection | to be measured | to be measured | ≥ 500k/s |
| Allocations per RPC, 1 KB | ~390 B | ~7.6 KB | < 500 B |
| Stream throughput, plain / TLS | — | 2.2 GB/s / 0.9 GB/s | ≥ 3 GB/s / ≥ 1.2 GB/s |

Run-to-run noise on this machine is large: the 8-connection PureGate result was 144k/s in one run and 94k/s in the next. Compare medians of several runs before claiming a gain.
