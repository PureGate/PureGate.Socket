# PureGate.Socket roadmap

What is still worth doing, most valuable first. Items that need measurements wait until benchmarking resumes.

---

## Protocol

The wire format is specified in [PROTOCOL.md](PROTOCOL.md).

- **Send-side compression with span-based Brotli and pooled buffers**, as receiving already does, instead of a `MemoryStream` per compressed payload.
- **Handshake binding:** derive a session key from the handshake nonces (HKDF). Optionally sign every frame when TLS isn't used.

## Allocations

- **No `SocketFrame` object on the hot path.** Use struct headers instead. A round trip allocates four frame objects today; this is the largest remaining internal allocation, but a large rewrite for the gain, and it changes no public API.

## Hardening

- **Coverage-guided fuzzing:** run SharpFuzz on the frame decoder, batch codecs, chunk reassembly and handshake, and add the crashes it finds to the seeded NUnit fuzz tests.
- **More chaos scenarios:** duplicate connections from one node ID, and peers that read very slowly at the socket level.
- **Branch coverage:** cover every decoder branch (branch coverage is 69% today, against 86% of lines), then gate branch coverage in CI as line coverage is.
- **Systematic concurrency testing** with Microsoft Coyote on the request and stream lifecycles (optional).

## When benchmarking resumes

- **Repeated runs:** run each scenario several times and report the median and spread. Single runs on this machine are too noisy to compare (see below).
- **Raw socket transport:** when there's no TLS, read and write the socket directly (the approach Kestrel uses) instead of through `NetworkStream`, keeping the stream path for TLS. Adopt it only if the benchmark proves the gain.
- **Cheaper compression:** benchmark Deflate against Brotli. Zstandard isn't in the .NET 10 base library and would need a third-party package; benchmark it before deciding whether that's worth a dependency.
- **Performance report:** compare the medians with the Phase 0 baseline and the raw-socket ceiling below.

---

## Performance targets

Targets are stated against the raw-socket ceiling measured on the same machine, because absolute numbers change from machine to machine. Baseline: [benchmarks/baseline-phase0.json](benchmarks/baseline-phase0.json) (Windows, 20 cores, .NET 10, loopback).

| Metric | Raw socket ceiling | PureGate.Socket baseline | Target |
|---|---|---|---|
| Sequential RPC, 1 KB | 50k/s, p50 20 µs | 25k/s (51%), p50 36 µs | ≥ 80% of ceiling |
| 8 connections, 1 KB | 203k/s | 94k/s (46%), p99 388 µs | ≥ 80% of ceiling, p99 within 2× of ceiling |
| 64 connections, 1 KB | 293k/s | 147k/s (50%), p99 3.1 ms | ≥ 80% of ceiling, p99 within 2× of ceiling |
| Pipelined, 1 connection | to be measured | to be measured | ≥ 500k/s |
| Allocations per RPC, 1 KB | ~390 B | ~7.6 KB | < 500 B |
| Stream throughput, plain / TLS | — | 2.2 GB/s / 0.9 GB/s | ≥ 3 GB/s / ≥ 1.2 GB/s |

Run-to-run noise on this machine is large: the 8-connection PureGate.Socket result was 144k/s in one run and 94k/s in the next. Compare medians of several runs before claiming a gain.
