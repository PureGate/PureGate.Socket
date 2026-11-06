using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Fabric;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Chaos tests: real traffic through a proxy that fragments, delays and resets connections. The contract
// under test: every call either succeeds with the right data or fails with a clean, documented error, and
// nothing hangs.
public sealed partial class SocketFabricTests
{
  static readonly string[] CleanFailureCodes =
  [
    SocketProtocolError.ConnectionClosed,
    SocketProtocolError.Timeout,
    SocketProtocolError.Cancelled
  ];

  [Test]
  [Category("Chaos")]
  public async Task Chaos_FragmentedAndDelayedTrafficIsDeliveredIntact()
  {
    await using var listener = await StartEchoListenerAsync();
    await using var proxy = ChaosProxy.Start(listener.EndPoint, new ChaosOptions { MaxFragmentBytes = 7, DelayOneIn = 500 });
    await using var client = await new SocketConnector(new SocketFabricOptions { NodeId = "client" })
        .ConnectAsync(proxy.EndPoint, new DelegateSocketPeerHandler());

    var random = new Random(FuzzSeed);
    var payloads = Enumerable.Range(0, 30).Select(_ => RandomBytes(random, random.Next(0, 20_000))).ToList();
    var echoes = await Task.WhenAll(payloads.Select(payload => client.RequestAsync("test", "echo", payload, TimeSpan.FromSeconds(20)).AsTask()));
    await using var stream = await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(20));
    var streamed = await ReadPatternAsync(stream);

    for (var i = 0; i < payloads.Count; i++)
    {
      Assert.That(echoes[i].Succeeded, Is.True, echoes[i].ErrorCode);
      Assert.That(echoes[i].Payload.ToArray(), Is.EqualTo(payloads[i]), $"payload {i} was corrupted");
    }

    Assert.That(streamed, Is.EqualTo(PatternStreamLength));
  }

  [Test]
  [Category("Chaos")]
  public async Task Chaos_ConnectionsResetMidFrameFailCleanlyAndRetriesGetThrough()
  {
    await using var listener = await StartEchoListenerAsync();
    await using var proxy = ChaosProxy.Start(listener.EndPoint, new ChaosOptions { MaxFragmentBytes = 512, ResetAfterBytes = (2_000, 60_000), Seed = FuzzSeed });
    await using var client = new SocketClient(
        proxy.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions
        {
          NodeId = "client",
          Codec = Serialization.RawSocketMessageCodec.Shared,
          RequestTimeout = TimeSpan.FromSeconds(2),
          LivenessProbeTimeout = TimeSpan.FromMilliseconds(500)
        });

    var random = new Random(FuzzSeed);
    var unexpected = new ConcurrentQueue<string>();
    var succeeded = 0;
    await Parallel.ForEachAsync(Enumerable.Range(0, 200), new ParallelOptions { MaxDegreeOfParallelism = 16 }, async (index, _) =>
    {
      var payload = BitConverter.GetBytes(index).Concat(new byte[index * 37 % 4000]).ToArray();
      try
      {
        var result = await client.RequestAsync("test", "echo", payload, new SocketClientRequestOptions
        {
          Idempotent = true,
          MaxAttempts = 10,
          RetryDelay = TimeSpan.FromMilliseconds(20)
        }).AsTask().WaitAsync(TimeSpan.FromSeconds(60));

        if (result.Succeeded)
        {
          if (!result.Payload.Span.SequenceEqual(payload))
            unexpected.Enqueue($"call {index}: wrong payload: expected {payload.Length} bytes for call {index}, got {result.Payload.Length} bytes starting {Convert.ToHexString(result.Payload.Span[..Math.Min(8, result.Payload.Length)])}");
          Interlocked.Increment(ref succeeded);
        }
        else if (!CleanFailureCodes.Contains(result.ErrorCode))
        {
          unexpected.Enqueue($"call {index}: {result.ErrorCode} {result.ErrorMessage}");
        }
      }
      catch (Exception ex) when (ex is SocketProtocolException protocol && CleanFailureCodes.Contains(protocol.ErrorCode)
          || ex is IOException or SocketException)
      {
      }
      catch (Exception ex)
      {
        unexpected.Enqueue($"call {index}: {ex.GetType().Name}: {ex.Message}");
      }
    });

    Assert.That(unexpected, Is.Empty, string.Join(Environment.NewLine, unexpected));
    Assert.That(proxy.Resets, Is.GreaterThan(0), "the proxy should have reset some connections");
    Assert.That(succeeded, Is.GreaterThan(150), "retries should get most calls through");
  }

  [Test]
  [Category("Chaos")]
  public async Task Chaos_ReconnectStormLeavesNoConnectionsBehind()
  {
    var connected = 0;
    var disconnected = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          Observer = new DelegateSocketObserver
          {
            OnPeerConnected = _ => Interlocked.Increment(ref connected),
            OnPeerDisconnected = (_, _) => Interlocked.Increment(ref disconnected)
          }
        },
        new DelegateSocketPeerHandler { OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload }) });
    await using var proxy = ChaosProxy.Start(listener.EndPoint, new ChaosOptions());

    var clients = Enumerable.Range(0, 10).Select(i => new SocketClient(
        proxy.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions { NodeId = $"client-{i}" },
        new SocketClientOptions { KeepConnected = true, ReconnectInitialDelay = TimeSpan.FromMilliseconds(10), ReconnectMaxDelay = TimeSpan.FromMilliseconds(50) })).ToList();
    try
    {
      for (var storm = 0; storm < 10; storm++)
      {
        await Task.Delay(100);
        proxy.ResetAll();
      }

      // After the storm every client reconnects and works.
      var results = await Task.WhenAll(clients.Select(client => client.RequestAsync("test", "echo", 1, new SocketClientRequestOptions
      {
        Idempotent = true,
        MaxAttempts = 20,
        RetryDelay = TimeSpan.FromMilliseconds(20)
      }).AsTask()));
      Assert.That(results.Select(result => result.Succeeded), Has.All.True);
    }
    finally
    {
      foreach (var client in clients)
        await client.DisposeAsync();
    }

    await WaitUntilAsync(() => Volatile.Read(ref connected) == Volatile.Read(ref disconnected), TimeSpan.FromSeconds(10));
    Assert.That(connected, Is.GreaterThan(10), "the storm should have forced reconnects");
    Assert.That(disconnected, Is.EqualTo(connected), "every server-side connection should be closed");
  }

  const long PatternStreamLength = 256 * 1024;

  static async ValueTask<SocketListener> StartEchoListenerAsync()
  {
    return await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload }),
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(PatternStreamLength)))
        });
  }

  static async ValueTask<long> ReadPatternAsync(Stream stream)
  {
    var buffer = new byte[7919];
    long position = 0;
    int read;
    while ((read = await stream.ReadAsync(buffer)) > 0)
    {
      for (var i = 0; i < read; i++)
      {
        if (buffer[i] != PatternStream.ByteAt(position + i))
          Assert.Fail($"stream corrupted at offset {position + i}");
      }

      position += read;
    }

    return position;
  }

  sealed class ChaosOptions
  {
    // Forwarded writes are split into pieces of 1 to MaxFragmentBytes bytes (0: forward as read).
    public int MaxFragmentBytes { get; init; }
    // About one forwarded piece in DelayOneIn waits a millisecond (in practice a timer tick, ~15 ms on
    // Windows), so delays are rare enough not to dominate the test. 0: no delays.
    public int DelayOneIn { get; init; }
    // Each connection is reset (RST) after forwarding a random number of bytes in this range.
    public (int Min, int Max)? ResetAfterBytes { get; init; }
    public int Seed { get; init; } = 1;
  }

  // A TCP proxy that misbehaves on purpose. Resets are abortive (RST, no FIN), like a crashed peer or a
  // middlebox dropping the connection.
  sealed class ChaosProxy : IAsyncDisposable
  {
    readonly TcpListener Listener = new(IPAddress.Loopback, 0);
    readonly IPEndPoint Upstream;
    readonly ChaosOptions Options;
    readonly ConcurrentDictionary<int, (TcpClient Downstream, TcpClient Upstream)> Connections = new();
    readonly CancellationTokenSource Lifetime = new();
    readonly Random Random;
    Task AcceptLoop;
    int NextConnection;
    int ResetCount;

    ChaosProxy(IPEndPoint upstream, ChaosOptions options)
    {
      Upstream = upstream;
      Options = options;
      Random = new Random(options.Seed);
    }

    public IPEndPoint EndPoint => (IPEndPoint)Listener.LocalEndpoint;
    public int Resets => Volatile.Read(ref ResetCount);

    public static ChaosProxy Start(IPEndPoint upstream, ChaosOptions options)
    {
      var proxy = new ChaosProxy(upstream, options);
      proxy.Listener.Start();
      proxy.AcceptLoop = Task.Run(proxy.AcceptAsync);
      return proxy;
    }

    public void ResetAll()
    {
      foreach (var id in Connections.Keys)
        Reset(id);
    }

    async Task AcceptAsync()
    {
      try
      {
        while (true)
        {
          var downstream = await Listener.AcceptTcpClientAsync(Lifetime.Token);
          var upstream = new TcpClient(AddressFamily.InterNetwork) { NoDelay = true };
          try
          {
            await upstream.ConnectAsync(Upstream, Lifetime.Token);
          }
          catch
          {
            downstream.Dispose();
            upstream.Dispose();
            continue;
          }

          var id = Interlocked.Increment(ref NextConnection);
          Connections[id] = (downstream, upstream);
          long budget;
          lock (Random)
            budget = Options.ResetAfterBytes is { } range ? Random.Next(range.Min, range.Max) : long.MaxValue;
          var shared = new StrongBox(budget);
          _ = PumpAsync(id, downstream.GetStream(), upstream.GetStream(), shared);
          _ = PumpAsync(id, upstream.GetStream(), downstream.GetStream(), shared);
        }
      }
      catch
      {
      }
    }

    async Task PumpAsync(int id, NetworkStream from, NetworkStream to, StrongBox budget)
    {
      var buffer = new byte[64 * 1024];
      try
      {
        while (true)
        {
          var read = await from.ReadAsync(buffer, Lifetime.Token);
          if (read == 0)
          {
            Reset(id);
            return;
          }

          for (var offset = 0; offset < read;)
          {
            int piece, delay;
            lock (Random)
            {
              piece = Options.MaxFragmentBytes > 0 ? Math.Min(read - offset, 1 + Random.Next(Options.MaxFragmentBytes)) : read - offset;
              delay = Options.DelayOneIn > 0 && Random.Next(Options.DelayOneIn) == 0 ? 1 : 0;
            }

            if (Interlocked.Add(ref budget.Value, -piece) < 0)
            {
              Reset(id);
              return;
            }

            if (delay > 0)
              await Task.Delay(delay, Lifetime.Token);
            await to.WriteAsync(buffer.AsMemory(offset, piece), Lifetime.Token);
            offset += piece;
          }
        }
      }
      catch
      {
        Reset(id);
      }
    }

    void Reset(int id)
    {
      if (!Connections.TryRemove(id, out var pair))
        return;

      Interlocked.Increment(ref ResetCount);
      foreach (var client in new[] { pair.Downstream, pair.Upstream })
      {
        try
        {
          client.Client.LingerState = new LingerOption(true, 0);
        }
        catch
        {
        }

        client.Dispose();
      }
    }

    public async ValueTask DisposeAsync()
    {
      Lifetime.Cancel();
      Listener.Stop();
      foreach (var id in Connections.Keys)
        Reset(id);
      await AcceptLoop;
      Lifetime.Dispose();
    }

    sealed class StrongBox(long value)
    {
      public long Value = value;
    }
  }
}
