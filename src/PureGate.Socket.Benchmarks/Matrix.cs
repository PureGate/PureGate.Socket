using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;
using PureGate.Socket.Transport;

// The matrix suite: latency percentiles, payload sizes, connection scaling, TLS and compression variants,
// and a raw-socket ceiling so every PureGate number can be read against what the machine can do.
static class MatrixBenchmarks
{
  static readonly int[] PayloadSizes = [64, 1024, 64 * 1024, 1024 * 1024];
  static readonly int[] ConnectionCounts = [1, 8, 64];
  static readonly int[] MultiplexedCallers = [8, 64];

  public static async Task<List<BenchmarkResult>> RunAsync(bool quick, string filter = null)
  {
    Filter = filter;
    var results = new List<BenchmarkResult>();
    try
    {
      await RunScenariosAsync(results, quick);
    }
    finally
    {
      results.RemoveAll(result => result is null);
    }

    return results;
  }

  static async Task RunScenariosAsync(List<BenchmarkResult> results, bool quick)
  {
    var scale = quick ? 10 : 1;
    using var certificate = CreateCertificate();

    Console.WriteLine("Matrix: raw socket ceiling");
    foreach (var size in PayloadSizes)
      results.Add(await MeasureSequentialAsync($"raw socket echo {Size(size)}", Iterations(size) / scale, size, RawEchoSession.StartAsync));
    foreach (var connections in ConnectionCounts)
      results.Add(await MeasureConcurrentAsync($"raw socket echo 1KB x{connections} conn", connections, 20_000 / scale, 1024, RawEchoSession.StartAsync));

    Console.WriteLine("Matrix: PureGate RPC");
    foreach (var size in PayloadSizes)
      results.Add(await MeasureSequentialAsync($"rpc echo {Size(size)}", Iterations(size) / scale, size, () => PeerEchoSession.StartAsync(Variant.Plain, certificate)));
    foreach (var size in PayloadSizes)
      results.Add(await MeasureSequentialAsync($"fast rpc echo {Size(size)}", Iterations(size) / scale, size, () => PeerEchoSession.StartAsync(Variant.Plain, certificate, fast: true)));
    foreach (var connections in ConnectionCounts)
      results.Add(await MeasureConcurrentAsync($"rpc echo 1KB x{connections} conn", connections, 20_000 / scale, 1024, () => PeerEchoSession.StartAsync(Variant.Plain, certificate)));
    foreach (var callers in MultiplexedCallers)
      results.Add(await MeasureMultiplexedAsync($"rpc echo 1KB 1 conn x{callers} callers", callers, 40_000 / scale, 1024, () => PeerEchoSession.StartAsync(Variant.Plain, certificate)));

    Console.WriteLine("Matrix: TLS and compression");
    foreach (var variant in new[] { Variant.Tls, Variant.Brotli, Variant.TlsBrotli })
    {
      results.Add(await MeasureSequentialAsync($"rpc echo 1KB [{variant}]", 5_000 / scale, 1024, () => PeerEchoSession.StartAsync(variant, certificate)));
      results.Add(await MeasureSequentialAsync($"rpc echo 64KB text [{variant}]", 1_000 / scale, 64 * 1024, () => PeerEchoSession.StartAsync(variant, certificate), compressible: true));
      results.Add(await MeasureConcurrentAsync($"rpc echo 1KB x8 conn [{variant}]", 8, 20_000 / scale, 1024, () => PeerEchoSession.StartAsync(variant, certificate)));
    }

    Console.WriteLine("Matrix: streams");
    foreach (var variant in new[] { Variant.Plain, Variant.Tls })
      results.Add(await MeasureStreamAsync($"pull stream 2GB [{variant}]", (quick ? 256L : 2048L) * 1024 * 1024, variant, certificate));
  }

  static int Iterations(int payloadSize)
  {
    return payloadSize switch
    {
      <= 1024 => 10_000,
      <= 64 * 1024 => 2_000,
      _ => 200
    };
  }

  static string Size(int bytes)
  {
    return bytes >= 1024 * 1024 ? $"{bytes / (1024 * 1024)}MB" : bytes >= 1024 ? $"{bytes / 1024}KB" : $"{bytes}B";
  }

  // Scenarios whose name does not contain this text are skipped (--filter).
  static string Filter;

  // A scenario that fails is recorded as failed instead of aborting the run, so a broken feature shows
  // up in the report next to the numbers that did complete.
  static async Task<BenchmarkResult> GuardAsync(string name, Func<Task<BenchmarkResult>> run)
  {
    if (Filter is not null && !name.Contains(Filter, StringComparison.OrdinalIgnoreCase))
      return null;

    try
    {
      return await run().WaitAsync(TimeSpan.FromMinutes(2));
    }
    catch (Exception ex)
    {
      var result = BenchmarkResult.Failed(name, $"{ex.GetType().Name}: {ex.Message}");
      Console.WriteLine(result.ToConsoleLine());
      return result;
    }
  }

  static Task<BenchmarkResult> MeasureSequentialAsync(
      string name,
      int iterations,
      int payloadSize,
      Func<ValueTask<IEchoSession>> start,
      bool compressible = false)
  {
    return GuardAsync(name, () => MeasureSequentialCoreAsync(name, iterations, payloadSize, start, compressible));
  }

  static Task<BenchmarkResult> MeasureConcurrentAsync(
      string name,
      int connections,
      int totalOperations,
      int payloadSize,
      Func<ValueTask<IEchoSession>> start)
  {
    return GuardAsync(name, () => MeasureConcurrentCoreAsync(name, connections, totalOperations, payloadSize, start));
  }

  // Many callers sharing one connection: the usual cluster pattern, where several frames are often queued
  // for the same socket at once.
  static Task<BenchmarkResult> MeasureMultiplexedAsync(
      string name,
      int callers,
      int totalOperations,
      int payloadSize,
      Func<ValueTask<IEchoSession>> start)
  {
    return GuardAsync(name, async () =>
    {
      var perCaller = Math.Max(totalOperations / callers, 20);
      await using var session = await start();
      var echo = await session.CreateEchoAsync();
      var payload = CreatePayload(payloadSize, compressible: false);
      var latencies = new long[perCaller * callers];
      var measurement = Measurement.Start();
      await Task.WhenAll(Enumerable.Range(0, callers).Select(caller => Task.Run(async () =>
      {
        var offset = caller * perCaller;
        for (var i = 0; i < perCaller; i++)
        {
          var started = Stopwatch.GetTimestamp();
          await echo(payload);
          latencies[offset + i] = Stopwatch.GetTimestamp() - started;
        }
      })));

      return measurement.Stop(name, latencies.Length, latencies, (long)latencies.Length * payloadSize);
    });
  }

  static Task<BenchmarkResult> MeasureStreamAsync(string name, long bytes, Variant variant, X509Certificate2 certificate)
  {
    return GuardAsync(name, () => MeasureStreamCoreAsync(name, bytes, variant, certificate));
  }

  static async Task<BenchmarkResult> MeasureSequentialCoreAsync(
      string name,
      int iterations,
      int payloadSize,
      Func<ValueTask<IEchoSession>> start,
      bool compressible = false)
  {
    iterations = Math.Max(iterations, 20);
    await using var session = await start();
    var echo = await session.CreateEchoAsync();
    var payload = CreatePayload(payloadSize, compressible);
    var latencies = new long[iterations];
    var measurement = Measurement.Start();
    for (var i = 0; i < iterations; i++)
    {
      var started = Stopwatch.GetTimestamp();
      await echo(payload);
      latencies[i] = Stopwatch.GetTimestamp() - started;
    }

    return measurement.Stop(name, iterations, latencies, (long)iterations * payloadSize);
  }

  static async Task<BenchmarkResult> MeasureConcurrentCoreAsync(
      string name,
      int connections,
      int totalOperations,
      int payloadSize,
      Func<ValueTask<IEchoSession>> start)
  {
    var perConnection = Math.Max(totalOperations / connections, 20);
    await using var session = await start();
    var echoes = new Func<byte[], ValueTask>[connections];
    for (var i = 0; i < connections; i++)
      echoes[i] = await session.CreateEchoAsync();
    var payload = CreatePayload(payloadSize, compressible: false);
    var latencies = new long[perConnection * connections];
    var measurement = Measurement.Start();
    await Task.WhenAll(echoes.Select((echo, connection) => Task.Run(async () =>
    {
      var offset = connection * perConnection;
      for (var i = 0; i < perConnection; i++)
      {
        var started = Stopwatch.GetTimestamp();
        await echo(payload);
        latencies[offset + i] = Stopwatch.GetTimestamp() - started;
      }
    })));

    return measurement.Stop(name, latencies.Length, latencies, (long)latencies.Length * payloadSize);
  }

  static async Task<BenchmarkResult> MeasureStreamCoreAsync(string name, long bytes, Variant variant, X509Certificate2 certificate)
  {
    var options = PeerEchoSession.Options("server", variant, certificate);
    await using var listener = new SocketListener(options);
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new BenchmarkHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new RepeatingByteStream(bytes)))
        });
    await using var client = await new SocketConnector(PeerEchoSession.Options("client", variant, certificate))
        .ConnectAsync(listener.EndPoint, new BenchmarkHandler());

    var measurement = Measurement.Start();
    await ReadAllAsync(client, bytes);
    return measurement.Stop(name, 1, null, bytes, unit: "bytes");
  }

  static async Task ReadAllAsync(SocketPeer client, long expected)
  {
    await using var stream = await client.OpenStreamAsync("bench", "stream", [], TimeSpan.FromSeconds(30));
    var buffer = new byte[256 * 1024];
    long total = 0;
    int read;
    while ((read = await stream.ReadAsync(buffer)) > 0)
      total += read;
    if (total != expected)
      throw new InvalidOperationException($"Stream returned {total} bytes, expected {expected}.");
  }

  static byte[] CreatePayload(int size, bool compressible)
  {
    var payload = new byte[size];
    if (!compressible)
    {
      Random.Shared.NextBytes(payload);
      return payload;
    }

    var words = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "u8;
    for (var i = 0; i < size; i++)
      payload[i] = words[(i * 7 + i / 13) % words.Length];
    return payload;
  }

  static X509Certificate2 CreateCertificate()
  {
    using var rsa = RSA.Create(2048);
    var request = new CertificateRequest("CN=puregate.socket", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], false));
    var names = new SubjectAlternativeNameBuilder();
    names.AddDnsName("puregate.socket");
    request.CertificateExtensions.Add(names.Build());
    using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pkcs12), null, X509KeyStorageFlags.Exportable);
  }

  public static void WriteJson(string path, IReadOnlyList<BenchmarkResult> results)
  {
    var report = new
    {
      createdUtc = DateTime.UtcNow,
      runtime = Environment.Version.ToString(),
      os = RuntimeInformation.OSDescription,
      architecture = RuntimeInformation.ProcessArchitecture.ToString(),
      processorCount = Environment.ProcessorCount,
      results
    };
    var directory = Path.GetDirectoryName(Path.GetFullPath(path));
    if (!string.IsNullOrEmpty(directory))
      Directory.CreateDirectory(directory);
    File.WriteAllText(path, JsonSerializer.Serialize(report, new JsonSerializerOptions
    {
      WriteIndented = true,
      PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    }) + "\n");
    Console.WriteLine($"Wrote {results.Count} results to {path}");
  }

  enum Variant
  {
    Plain,
    Tls,
    Brotli,
    TlsBrotli
  }

  interface IEchoSession : IAsyncDisposable
  {
    ValueTask<Func<byte[], ValueTask>> CreateEchoAsync();
  }

  sealed class PeerEchoSession(SocketListener listener, Variant variant, X509Certificate2 certificate, bool fast) : IEchoSession
  {
    readonly List<SocketPeer> Clients = [];

    public static async ValueTask<IEchoSession> StartAsync(Variant variant, X509Certificate2 certificate, bool fast = false)
    {
      var listener = new SocketListener(Options("server", variant, certificate));
      await listener.StartAsync(
          new IPEndPoint(IPAddress.Loopback, 0),
          new BenchmarkHandler
          {
            OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload }),
            OnFastRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload })
          });
      return new PeerEchoSession(listener, variant, certificate, fast);
    }

    public static SocketFabricOptions Options(string nodeId, Variant variant, X509Certificate2 certificate)
    {
      var tls = variant is Variant.Tls or Variant.TlsBrotli;
      var brotli = variant is Variant.Brotli or Variant.TlsBrotli;
      return new SocketFabricOptions
      {
        NodeId = nodeId,
        Codec = RawSocketMessageCodec.Shared,
        MaxFrameSizeBytes = 4 * 1024 * 1024,
        MaxConcurrentHandlers = Environment.ProcessorCount * 2,
        MaxQueuedHandlers = 100_000,
        MaxInFlightRequests = 100_000,
        StreamChunkBytes = 128 * 1024,
        MaxStreamBufferChunks = 64,
        RequestTimeout = TimeSpan.FromSeconds(30),
        PayloadCompression = brotli ? SocketPayloadCompression.Brotli : SocketPayloadCompression.None,
        StreamFilter = tls
            ? TlsSocketStreamFilter.Create(certificate, "puregate.socket", serverCertificateValidation: (_, _, _, _) => true)
            : null
      };
    }

    public async ValueTask<Func<byte[], ValueTask>> CreateEchoAsync()
    {
      var client = await new SocketConnector(Options("client", variant, certificate))
          .ConnectAsync(listener.EndPoint, new BenchmarkHandler());
      Clients.Add(client);
      var timeout = TimeSpan.FromSeconds(30);
      return fast
          ? async payload => Check(await client.RequestFastAsync(1, payload, timeout), payload.Length)
          : async payload => Check(await client.RequestAsync("bench", "echo", payload, timeout), payload.Length);
    }

    static void Check(SocketRequestResult result, int expectedLength)
    {
      if (!result.Succeeded || result.Payload.Length != expectedLength)
        throw new InvalidOperationException($"Echo failed: {result.ErrorCode} {result.ErrorMessage}");
    }

    public async ValueTask DisposeAsync()
    {
      foreach (var client in Clients)
        await client.DisposeAsync();
      await listener.DisposeAsync();
    }
  }

  // Length-prefixed echo over plain sockets: the ceiling for any framing layer on this machine.
  sealed class RawEchoSession(Socket listener, Task acceptLoop, CancellationTokenSource lifetime) : IEchoSession
  {
    readonly List<Socket> Sockets = [];

    public static ValueTask<IEchoSession> StartAsync()
    {
      var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      listener.Listen(512);
      var lifetime = new CancellationTokenSource();
      var acceptLoop = Task.Run(async () =>
      {
        try
        {
          while (true)
          {
            var socket = await listener.AcceptAsync(lifetime.Token);
            socket.NoDelay = true;
            _ = Task.Run(() => ServeAsync(socket, lifetime.Token));
          }
        }
        catch
        {
        }
      });
      return ValueTask.FromResult<IEchoSession>(new RawEchoSession(listener, acceptLoop, lifetime));
    }

    static async Task ServeAsync(Socket socket, CancellationToken cancellationToken)
    {
      using (socket)
      {
        // Header and payload share one buffer so each echo is a single send, as a well-built framing layer would do.
        var buffer = new byte[4 + 4 * 1024 * 1024];
        try
        {
          while (true)
          {
            await ReceiveExactlyAsync(socket, buffer.AsMemory(0, 4), cancellationToken);
            var length = BinaryPrimitives.ReadInt32BigEndian(buffer);
            await ReceiveExactlyAsync(socket, buffer.AsMemory(4, length), cancellationToken);
            await SendAllAsync(socket, buffer.AsMemory(0, 4 + length), cancellationToken);
          }
        }
        catch
        {
        }
      }
    }

    public async ValueTask<Func<byte[], ValueTask>> CreateEchoAsync()
    {
      var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
      await socket.ConnectAsync(listener.LocalEndPoint!);
      Sockets.Add(socket);
      var request = new byte[4 + 4 * 1024 * 1024];
      var response = new byte[4 + 4 * 1024 * 1024];
      return async payload =>
      {
        BinaryPrimitives.WriteInt32BigEndian(request, payload.Length);
        payload.CopyTo(request, 4);
        await SendAllAsync(socket, request.AsMemory(0, 4 + payload.Length), CancellationToken.None);
        await ReceiveExactlyAsync(socket, response.AsMemory(0, 4), CancellationToken.None);
        await ReceiveExactlyAsync(socket, response.AsMemory(4, BinaryPrimitives.ReadInt32BigEndian(response)), CancellationToken.None);
      };
    }

    static async ValueTask ReceiveExactlyAsync(Socket socket, Memory<byte> buffer, CancellationToken cancellationToken)
    {
      while (buffer.Length > 0)
      {
        var read = await socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
        if (read == 0)
          throw new IOException("Connection closed.");
        buffer = buffer[read..];
      }
    }

    static async ValueTask SendAllAsync(Socket socket, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
    {
      while (buffer.Length > 0)
        buffer = buffer[await socket.SendAsync(buffer, SocketFlags.None, cancellationToken)..];
    }

    public async ValueTask DisposeAsync()
    {
      lifetime.Cancel();
      foreach (var socket in Sockets)
        socket.Dispose();
      listener.Dispose();
      await acceptLoop;
      lifetime.Dispose();
    }
  }

  readonly struct Measurement
  {
    readonly long AllocatedBefore;
    readonly int Gen0;
    readonly int Gen1;
    readonly int Gen2;
    readonly long Started;

    Measurement(long allocatedBefore, int gen0, int gen1, int gen2, long started)
    {
      AllocatedBefore = allocatedBefore;
      Gen0 = gen0;
      Gen1 = gen1;
      Gen2 = gen2;
      Started = started;
    }

    public static Measurement Start()
    {
      GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
      GC.WaitForPendingFinalizers();
      return new Measurement(
          GC.GetTotalAllocatedBytes(precise: true),
          GC.CollectionCount(0),
          GC.CollectionCount(1),
          GC.CollectionCount(2),
          Stopwatch.GetTimestamp());
    }

    public BenchmarkResult Stop(string name, long operations, long[] latencies, long bytes, string unit = "ops")
    {
      var elapsed = Stopwatch.GetElapsedTime(Started);
      var allocated = GC.GetTotalAllocatedBytes(precise: true) - AllocatedBefore;
      var seconds = Math.Max(elapsed.TotalSeconds, 0.000001);
      double? p50 = null, p99 = null, p999 = null;
      if (latencies is { Length: > 0 })
      {
        Array.Sort(latencies);
        p50 = Percentile(latencies, 0.50);
        p99 = Percentile(latencies, 0.99);
        p999 = Percentile(latencies, 0.999);
      }

      var result = new BenchmarkResult(
          name,
          operations,
          Math.Round(elapsed.TotalMilliseconds, 2),
          Math.Round(unit == "bytes" ? bytes / seconds : operations / seconds, 0),
          unit,
          Math.Round(bytes / seconds / (1024 * 1024), 1),
          Math.Round((double)allocated / Math.Max(unit == "bytes" ? bytes : operations, 1), 2),
          p50,
          p99,
          p999,
          GC.CollectionCount(0) - Gen0,
          GC.CollectionCount(1) - Gen1,
          GC.CollectionCount(2) - Gen2);
      Console.WriteLine(result.ToConsoleLine());
      return result;
    }

    static double Percentile(long[] sorted, double percentile)
    {
      var index = (int)Math.Min(sorted.Length - 1, Math.Ceiling(percentile * sorted.Length) - 1);
      return Math.Round(sorted[Math.Max(index, 0)] * 1_000_000.0 / Stopwatch.Frequency, 1);
    }
  }
}

sealed record BenchmarkResult(
    string Name,
    long Operations,
    double ElapsedMs,
    double Throughput,
    string Unit,
    double MiBPerSecond,
    double AllocatedPerUnit,
    double? P50Us,
    double? P99Us,
    double? P999Us,
    int Gen0,
    int Gen1,
    int Gen2,
    string Error = null)
{
  public static BenchmarkResult Failed(string name, string error)
  {
    return new BenchmarkResult(name, 0, 0, 0, "ops", 0, 0, null, null, null, 0, 0, 0, error);
  }

  public string ToConsoleLine()
  {
    if (Error is not null)
      return $"  {Name,-38} FAILED: {Error}";

    var latency = P50Us is null ? string.Empty : $"  p50 {P50Us,8:N1}us  p99 {P99Us,8:N1}us  p99.9 {P999Us,8:N1}us";
    return $"  {Name,-38} {Throughput,14:N0} {Unit}/s  {MiBPerSecond,8:N1} MiB/s  alloc {AllocatedPerUnit,10:N1} B/{(Unit == "bytes" ? "byte" : "op")}{latency}  gc {Gen0}/{Gen1}/{Gen2}";
  }
}
