using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using PureGate.Socket.Observability;
using PureGate.Socket.Protocol;
using PureGate.Socket.Security;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Production readiness: the per-connection memory budget, decompression limits, and metrics.
public sealed partial class SocketFabricTests
{
  [Test]
  [Category("Memory")]
  public async Task MemoryBudget_RequestsPastTheBudgetAreRefusedAndTheRestSucceed()
  {
    var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    SocketPeer server = null;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          MaxConnectionMemoryBytes = 1024 * 1024,
          MaxConcurrentHandlers = 1,
          Observer = new DelegateSocketObserver { OnPeerConnected = peer => server = peer }
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, request, _) =>
          {
            await gate.Task;
            return new SocketResponse { Succeeded = true, Payload = BitConverter.GetBytes(request.Payload.Length) };
          }
        });
    await using var client = await ConnectAsync(listener);

    // One runs and holds its payload, two wait in the queue: 900 KB of the 1 MB budget. The fourth does
    // not fit.
    var calls = Enumerable.Range(0, 4)
        .Select(_ => client.RequestAsync("test", "hold", new byte[300_000], TimeSpan.FromSeconds(10)).AsTask())
        .ToList();
    var refused = await Task.WhenAny(calls).WaitAsync(TimeSpan.FromSeconds(5));
    Assert.That((await refused).ErrorCode, Is.EqualTo(SocketProtocolError.Overloaded));

    gate.SetResult();
    var results = await Task.WhenAll(calls);
    Assert.That(results.Count(result => result.Succeeded), Is.EqualTo(3));

    await WaitUntilAsync(() => server.Diagnostics.MemoryInUse == 0, TimeSpan.FromSeconds(5));
    Assert.That(server.Diagnostics.MemoryInUse, Is.Zero, "every reservation should be returned");
    var after = await client.RequestAsync("test", "hold", new byte[300_000], TimeSpan.FromSeconds(10));
    Assert.That(after.Succeeded, Is.True, after.ErrorCode);
  }

  [Test]
  [Category("Memory")]
  public async Task MemoryBudget_ChunkedRequestThatDoesNotFitIsRefusedAndTheConnectionSurvives()
  {
    SocketPeer server = null;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          MaxFrameSizeBytes = 64 * 1024,
          MaxConnectionMemoryBytes = 1024 * 1024,
          Observer = new DelegateSocketObserver { OnPeerConnected = peer => server = peer }
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload })
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", MaxFrameSizeBytes = 64 * 1024 });

    var tooLarge = await client.RequestAsync("test", "echo", new byte[2 * 1024 * 1024], TimeSpan.FromSeconds(10));
    Assert.That(tooLarge.ErrorCode, Is.EqualTo(SocketProtocolError.Overloaded));

    var fits = RandomBytes(new Random(FuzzSeed), 512 * 1024);
    var echoed = await client.RequestAsync("test", "echo", fits, TimeSpan.FromSeconds(10));
    Assert.That(echoed.Succeeded, Is.True, echoed.ErrorCode);
    Assert.That(echoed.Payload.ToArray(), Is.EqualTo(fits));
    await WaitUntilAsync(() => server.Diagnostics.MemoryInUse == 0, TimeSpan.FromSeconds(5));
    Assert.That(server.Diagnostics.MemoryInUse, Is.Zero);
  }

  [Test]
  [Category("Memory")]
  public async Task MemoryBudget_EventThatDoesNotFitClosesTheConnection()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConnectionMemoryBytes = 64 * 1024 },
        new DelegateSocketPeerHandler());
    await using var client = await ConnectAsync(listener);

    await client.SendEventAsync("test", "large", new byte[100_000]);

    await client.Completion.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Closed));
  }

  [Test]
  [Category("Memory")]
  public async Task MemoryBudget_UnreadStreamDataPastTheBudgetFailsAStreamNotTheConnection()
  {
    const long streamLength = 2 * 1024 * 1024;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(streamLength))),
          OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload })
        });
    // Three unread streams may buffer 3 x 384 KB, more than the 1 MB budget.
    await using var client = await ConnectAsync(listener, new SocketFabricOptions
    {
      NodeId = "client",
      MaxConnectionMemoryBytes = 1024 * 1024,
      StreamWindowBytes = 384 * 1024,
      MaxStreamWindowBytes = 384 * 1024
    });

    var streams = new List<Stream>();
    for (var i = 0; i < 3; i++)
      streams.Add(await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(10)));
    await WaitUntilAsync(() => client.Diagnostics.PendingStreams < 3, TimeSpan.FromSeconds(5));

    var overloaded = 0;
    var completed = 0;
    foreach (var stream in streams)
    {
      try
      {
        Assert.That(await ReadPatternAsync(stream), Is.EqualTo(streamLength));
        completed++;
      }
      catch (SocketProtocolException ex) when (ex.ErrorCode == SocketProtocolError.Overloaded)
      {
        overloaded++;
      }

      await stream.DisposeAsync();
    }

    Assert.That(overloaded, Is.GreaterThan(0), "a stream past the budget should fail with overloaded");
    Assert.That(completed, Is.GreaterThan(0), "streams that fit should be delivered intact");
    var echo = await client.RequestAsync("test", "echo", [1, 2, 3], TimeSpan.FromSeconds(5));
    Assert.That(echo.Succeeded, Is.True, "the connection should survive");
    await WaitUntilAsync(() => client.Diagnostics.MemoryInUse == 0, TimeSpan.FromSeconds(5));
    Assert.That(client.Diagnostics.MemoryInUse, Is.Zero);
  }

  [Test]
  [Category("Memory")]
  public async Task Compression_PayloadThatDecompressesPastTheLimitClosesTheConnection()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          PayloadCompression = SocketPayloadCompression.Brotli,
          MaxDecompressedPayloadSizeBytes = 1024 * 1024
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, _, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true })
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", PayloadCompression = SocketPayloadCompression.Brotli });

    // 10 MB of zeros compresses to a few hundred bytes.
    var result = await client.RequestAsync("test", "bomb", new byte[10 * 1024 * 1024], TimeSpan.FromSeconds(10));

    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.ConnectionClosed));
    await client.Completion.WaitAsync(TimeSpan.FromSeconds(5));
  }

  [Test]
  [Category("Memory")]
  public async Task Compression_PayloadThatDecompressesPastTheBudgetIsRefused()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          PayloadCompression = SocketPayloadCompression.Brotli,
          MaxConnectionMemoryBytes = 1024 * 1024
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = BitConverter.GetBytes(request.Payload.Length) })
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", PayloadCompression = SocketPayloadCompression.Brotli });

    var refused = await client.RequestAsync("test", "large", new byte[10 * 1024 * 1024], TimeSpan.FromSeconds(10));
    Assert.That(refused.ErrorCode, Is.EqualTo(SocketProtocolError.Overloaded));

    var fits = await client.RequestAsync("test", "small", new byte[256 * 1024], TimeSpan.FromSeconds(10));
    Assert.That(fits.Succeeded, Is.True, fits.ErrorCode);
    Assert.That(BitConverter.ToInt32(fits.Payload.Span), Is.EqualTo(256 * 1024));
  }

  [Test]
  [Category("Metrics")]
  public async Task Metrics_AreReportedThroughTheMeter()
  {
    var measurements = new ConcurrentQueue<(string Name, double Value, Dictionary<string, object> Tags)>();
    using var meterListener = new MeterListener();
    meterListener.InstrumentPublished = (instrument, listener) =>
    {
      if (instrument.Meter.Name == SocketMetrics.MeterName)
        listener.EnableMeasurementEvents(instrument);
    };
    meterListener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        measurements.Enqueue((instrument.Name, value, ToDictionary(tags))));
    meterListener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        measurements.Enqueue((instrument.Name, value, ToDictionary(tags))));
    meterListener.SetMeasurementEventCallback<int>((instrument, value, tags, _) =>
        measurements.Enqueue((instrument.Name, value, ToDictionary(tags))));
    meterListener.Start();

    var eventReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", Authenticator = new HmacSocketAuthenticator("secret") },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) => ValueTask.FromResult(request.Operation == "fail"
              ? SocketResponse.Failed("custom_error", "failed")
              : new SocketResponse { Succeeded = true, Payload = request.Payload }),
          OnEvent = (_, _, _) =>
          {
            eventReceived.TrySetResult();
            return ValueTask.CompletedTask;
          }
        });
    var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", Authenticator = new HmacSocketAuthenticator("secret") });

    Assert.That((await client.RequestAsync("orders", "create", [1, 2, 3], TimeSpan.FromSeconds(5))).Succeeded, Is.True);
    Assert.That((await client.RequestAsync("orders", "fail", [], TimeSpan.FromSeconds(5))).ErrorCode, Is.EqualTo("custom_error"));
    await client.SendEventAsync("orders", "created", [4]);
    await eventReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
    await Assert.CatchAsync(async () => await ConnectAsync(listener, new SocketFabricOptions { NodeId = "intruder", Authenticator = new HmacSocketAuthenticator("wrong") }));
    meterListener.RecordObservableInstruments();
    await client.DisposeAsync();
    await WaitUntilAsync(() => measurements.Any(m => m.Name == "puregate.socket.connections.closed"), TimeSpan.FromSeconds(5));

    var all = measurements.ToList();
    var clientDurations = all.Where(m => m.Name == "puregate.socket.client.request.duration").ToList();
    Assert.That(clientDurations, Has.Count.EqualTo(2));
    Assert.That(clientDurations.All(m => Equals(m.Tags["rpc.service"], "orders") && m.Value > 0), Is.True);
    Assert.That(clientDurations.Count(m => Equals(m.Tags.GetValueOrDefault("error.type"), "custom_error")), Is.EqualTo(1));
    Assert.That(all.Count(m => m.Name == "puregate.socket.server.request.duration"), Is.EqualTo(2));
    Assert.That(all.Count(m => m.Name == "puregate.socket.events.sent" && Equals(m.Tags["rpc.method"], "created")), Is.EqualTo(1));
    Assert.That(all.Count(m => m.Name == "puregate.socket.events.received"), Is.EqualTo(1));
    Assert.That(all.Any(m => m.Name == "puregate.socket.handshake.failures"
        && Equals(m.Tags["error.type"], SocketProtocolError.Unauthorized)), Is.True);
    Assert.That(all.Single(m => m.Name == "puregate.socket.connections").Value, Is.GreaterThanOrEqualTo(2));
    Assert.That(all.Single(m => m.Name == "puregate.socket.bytes.sent").Value, Is.GreaterThan(0));
    Assert.That(all.Single(m => m.Name == "puregate.socket.bytes.received").Value, Is.GreaterThan(0));
    Assert.That(all.Where(m => m.Name == "puregate.socket.streams.active").Select(m => m.Tags["direction"]), Is.EquivalentTo(new[] { "in", "out" }));
  }

  static Dictionary<string, object> ToDictionary(ReadOnlySpan<KeyValuePair<string, object>> tags)
  {
    var result = new Dictionary<string, object>();
    foreach (var tag in tags)
      result[tag.Key] = tag.Value;
    return result;
  }
}
