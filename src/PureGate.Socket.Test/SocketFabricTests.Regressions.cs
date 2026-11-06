using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Fabric;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;
using PureGate.Socket.Security;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Regression tests for the defects found in the November 2026 evaluation (docs/ROADMAP.md, Phase 0).
// Each test names the bug number it guards. Tests that need the Phase 2 connection-core rework are
// ignored with the reason, so the suite stays green while still documenting the open defect.
public sealed partial class SocketFabricTests
{
  const string Phase2FlowControl = "Phase 2: needs the single writer loop and credit-based flow control (docs/ROADMAP.md).";
  const string Phase2EventOrdering = "Phase 2: needs ordered event dispatch (docs/ROADMAP.md).";

  [Test]
  [Category("Regression")]
  public async Task Bug01_HandlerOperationCanceled_ReturnsHandlerFailedAndKeepsWorkerAlive()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1 },
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => request.Operation == "boom"
              ? throw new TaskCanceledException("simulated HttpClient timeout")
              : ValueTask.FromResult(SocketResponse.Success())
        });
    await using var client = await ConnectAsync(listener);

    var failed = await client.RequestAsync("test", "boom", [], TimeSpan.FromSeconds(2));
    var next = await client.RequestAsync("test", "ok", [], TimeSpan.FromSeconds(2));

    Assert.That(failed.ErrorCode, Is.EqualTo(SocketProtocolError.HandlerFailed));
    Assert.That(next.Succeeded, Is.True, $"worker died: {next.ErrorCode}");
  }

  [Test]
  [Category("Regression")]
  public async Task Bug01_FastAndStreamHandlerOperationCanceled_KeepWorkerAlive()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1 },
        new DelegateSocketPeerHandler
        {
          OnFastRequest = (_, _, _) => throw new OperationCanceledException("not ours"),
          OnStream = (_, _, _) => throw new OperationCanceledException("not ours"),
          OnEvent = (_, _, _) => throw new OperationCanceledException("not ours"),
          OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success())
        });
    await using var client = await ConnectAsync(listener);

    var fast = await client.RequestFastAsync(1, [], TimeSpan.FromSeconds(2));
    var streamError = await TryOpenStreamAsync(client);
    await client.SendEventAsync("test", "event", []);
    var next = await client.RequestAsync("test", "ok", [], TimeSpan.FromSeconds(2));

    Assert.That(fast.ErrorCode, Is.EqualTo(SocketProtocolError.HandlerFailed));
    Assert.That(streamError, Is.EqualTo(SocketProtocolError.HandlerFailed));
    Assert.That(next.Succeeded, Is.True, $"worker died: {next.ErrorCode}");
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug01_IdempotentRetryTakesOverWhenFirstAttemptIsCancelled()
  {
    var attempts = 0;
    var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, _, cancellationToken) =>
          {
            if (Interlocked.Increment(ref attempts) == 1)
            {
              firstStarted.TrySetResult();
              await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            return SocketResponse.Success();
          }
        });

    var first = await ConnectAsync(listener);
    var firstAttempt = first.RequestAsync("test", "op", [], TimeSpan.FromSeconds(10), idempotencyKey: "key-1").AsTask();
    await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

    await using var second = await ConnectAsync(listener);
    var retry = second.RequestAsync("test", "op", [], TimeSpan.FromSeconds(3), idempotencyKey: "key-1").AsTask();
    await Task.Delay(200);
    await first.DisposeAsync();
    await firstAttempt;

    var result = await retry;
    Assert.That(result.Succeeded, Is.True, $"retry did not take over: {result.ErrorCode}");
    Assert.That(attempts, Is.EqualTo(2));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug02_CompressionPreservesFastOperationId()
  {
    var seenOperations = new ConcurrentQueue<int>();
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", PayloadCompression = SocketPayloadCompression.Brotli },
        new DelegateSocketPeerHandler
        {
          OnFastRequest = (_, request, _) =>
          {
            seenOperations.Enqueue(request.OperationId);
            return ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload });
          }
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions
    {
      NodeId = "client",
      PayloadCompression = SocketPayloadCompression.Brotli
    });
    var compressible = new byte[64 * 1024];

    var single = await client.RequestFastAsync(42, compressible, TimeSpan.FromSeconds(5));
    var batch = await client.RequestFastBatchAsync(
        [new SocketFastBatchItem(1, 7, compressible)],
        TimeSpan.FromSeconds(5));

    Assert.That(single.Succeeded, Is.True);
    Assert.That(single.Payload.Length, Is.EqualTo(compressible.Length));
    Assert.That(batch, Has.Count.EqualTo(1));
    Assert.That(batch[0].Succeeded, Is.True);
    Assert.That(seenOperations, Is.EqualTo(new[] { 42, 7 }));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug03_FramesSentRightAfterHandshakeAreNotDropped()
  {
    const int connections = 20;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnConnect = async (peer, cancellationToken) => await peer.SendEventAsync("test", "welcome", [1], cancellationToken)
        });

    var delivered = 0;
    for (var i = 0; i < connections; i++)
    {
      var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var connector = new SocketConnector(new SocketFabricOptions
      {
        NodeId = "client",
        StreamFilter = new SlowReadStreamFilter(TimeSpan.FromMilliseconds(30))
      });
      await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler
      {
        OnEvent = (_, _, _) =>
        {
          received.TrySetResult();
          return ValueTask.CompletedTask;
        }
      });

      if (await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(2))) == received.Task)
        delivered++;
    }

    Assert.That(delivered, Is.EqualTo(connections));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug04_EventHandlerFailureDoesNotClosePeer()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, _, _) => throw new InvalidOperationException("bug in one event handler"),
          OnRequest = async (_, _, cancellationToken) =>
          {
            await Task.Delay(300, cancellationToken);
            return SocketResponse.Success();
          }
        });
    await using var client = await ConnectAsync(listener);

    var inFlight = client.RequestAsync("test", "slow", [], TimeSpan.FromSeconds(5)).AsTask();
    await Task.Delay(50);
    await client.SendEventAsync("test", "event", [1]);
    await client.SendEventBatchAsync([new SocketBatchItem(1, "test", "event", [1])]);
    var result = await inFlight;

    Assert.That(result.Succeeded, Is.True, $"in-flight request failed: {result.ErrorCode}");
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug05_StreamCanOutliveTheOpenTimeout()
  {
    const int chunks = 15;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(
              new SlowChunkStream(chunks, chunkBytes: 1024, delay: TimeSpan.FromMilliseconds(100))))
        });
    await using var client = await ConnectAsync(listener);

    await using var stream = await client.OpenStreamAsync("test", "slow", [], TimeSpan.FromMilliseconds(500));
    var total = await CountBytesAsync(stream);

    Assert.That(total, Is.EqualTo(chunks * 1024));
  }

  [Test]
  [Category("Regression")]
  [Ignore(Phase2FlowControl)]
  public async Task Bug06_UnreadStreamDoesNotStallOtherTraffic()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new MemoryStream(new byte[8 * 1024 * 1024]))),
          OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success())
        });
    await using var client = await ConnectAsync(listener);

    await using var stream = await client.OpenStreamAsync("test", "big", [], TimeSpan.FromSeconds(30));
    await Task.Delay(300);
    var result = await client.RequestAsync("test", "ping", [], TimeSpan.FromSeconds(2));

    Assert.That(result.Succeeded, Is.True, $"connection stalled: {result.ErrorCode}");
  }

  [Test]
  [Category("Regression")]
  public async Task Bug07_BatchEnvelopeRejectsItemCountLargerThanPayload()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler { OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success()) });
    await using var client = await ConnectAsync(listener);
    var envelope = new byte[4];
    BinaryPrimitives.WriteInt32BigEndian(envelope, 100_000_000);

    var before = GC.GetTotalAllocatedBytes(precise: true);
    var result = await client.RequestAsync("socket", "batch.request", envelope, TimeSpan.FromSeconds(10));
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

    Assert.That(result.Succeeded, Is.False);
    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.ProtocolError));
    Assert.That(allocated, Is.LessThan(16L * 1024 * 1024));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug08_ChunkCountInconsistentWithPayloadClosesConnectionWithoutAllocating()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler());
    using var tcp = new TcpClient(AddressFamily.InterNetwork);
    await tcp.ConnectAsync(listener.EndPoint);
    await RunRawNoAuthHandshakeAsync(tcp.GetStream());
    var writer = new SocketFrameWriter(tcp.GetStream(), 1024);

    var before = GC.GetTotalAllocatedBytes(precise: true);
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      MessageId = 1,
      Kind = SocketMessageKind.Chunk,
      OriginalKind = SocketMessageKind.Event,
      Service = "events",
      Operation = "one",
      ChunkIndex = 0,
      ChunkCount = 50_000_000,
      TotalPayloadLength = 1,
      Payload = [1]
    }, CancellationToken.None);
    var read = await tcp.GetStream().ReadAsync(new byte[1]).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

    Assert.That(read, Is.EqualTo(0), "server should close a connection that sends an inconsistent chunk");
    Assert.That(allocated, Is.LessThan(16L * 1024 * 1024));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug08_ChunkedMessageMemoryGrowsWithReceivedBytesNotDeclaredCount()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler());
    using var tcp = new TcpClient(AddressFamily.InterNetwork);
    await tcp.ConnectAsync(listener.EndPoint);
    await RunRawNoAuthHandshakeAsync(tcp.GetStream());
    var writer = new SocketFrameWriter(tcp.GetStream(), 1024);

    var before = GC.GetTotalAllocatedBytes(precise: true);
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      MessageId = 1,
      Kind = SocketMessageKind.Chunk,
      OriginalKind = SocketMessageKind.Event,
      Service = "events",
      Operation = "one",
      ChunkIndex = 0,
      ChunkCount = 50_000_000,
      TotalPayloadLength = 50_000_000,
      Payload = [1]
    }, CancellationToken.None);
    await Task.Delay(300);
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

    Assert.That(allocated, Is.LessThan(16L * 1024 * 1024));
  }

  [Test]
  [Category("Regression")]
  [Ignore(Phase2FlowControl)]
  public async Task Bug09_EventBurstAppliesBackpressureInsteadOfClosing()
  {
    const int events = 50;
    var handled = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1, MaxQueuedHandlers = 8 },
        new DelegateSocketPeerHandler
        {
          OnEvent = async (_, _, cancellationToken) =>
          {
            await Task.Delay(20, cancellationToken);
            Interlocked.Increment(ref handled);
          }
        });
    await using var client = await ConnectAsync(listener);

    for (var i = 0; i < events; i++)
      await client.SendEventAsync("test", "event", [1]);
    await WaitUntilAsync(() => Volatile.Read(ref handled) == events, TimeSpan.FromSeconds(10));

    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
    Assert.That(handled, Is.EqualTo(events));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug10_ClientRetryDoesNotCloseConnectionSharedByOtherCallers()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, request, cancellationToken) =>
          {
            await Task.Delay(request.Operation == "slow" ? 2000 : 400, cancellationToken);
            return SocketResponse.Success();
          }
        });
    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions { NodeId = "client", RequestTimeout = TimeSpan.FromMilliseconds(500) });
    var peer = await client.GetPeerAsync();

    var slow = client.RequestAsync("test", "slow", 1, new SocketClientRequestOptions
    {
      Idempotent = true,
      MaxAttempts = 2,
      RetryDelay = TimeSpan.Zero
    }).AsTask();
    await Task.Delay(250);
    var other = await client.RequestAsync("test", "normal", 1);
    await slow;

    Assert.That(other.Succeeded, Is.True, $"shared connection was closed: {other.ErrorCode}");
    Assert.That(peer.State, Is.EqualTo(SocketPeerState.Connected));
  }

  [Test]
  [Category("Regression")]
  public async Task LocalOverloadIsReportedAsOverloaded()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1, MaxQueuedHandlers = 1 },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, _, cancellationToken) =>
          {
            await Task.Delay(300, cancellationToken);
            return SocketResponse.Success();
          }
        });
    await using var limited = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "limited", MaxInFlightRequests = 1 });
    await using var flooding = await ConnectAsync(listener);

    var held = limited.RequestAsync("test", "hold", [], TimeSpan.FromSeconds(5)).AsTask();
    var rejectedLocally = await limited.RequestAsync("test", "second", [], TimeSpan.FromSeconds(5));
    var flood = await Task.WhenAll(Enumerable.Range(0, 6)
        .Select(_ => flooding.RequestAsync("test", "flood", [], TimeSpan.FromSeconds(5)).AsTask()));
    await held;

    Assert.That(rejectedLocally.ErrorCode, Is.EqualTo("overloaded"));
    Assert.That(flood.Select(result => result.ErrorCode), Has.Some.EqualTo("overloaded"));
  }

  [Test]
  [Category("Regression")]
  public async Task HandlerExceptionDetailsAreNotSentToRemotePeerByDefault()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, _, _) => throw new InvalidOperationException("connection string password=hunter2")
        });
    await using var client = await ConnectAsync(listener);

    var result = await client.RequestAsync("test", "op", [], TimeSpan.FromSeconds(5));

    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.HandlerFailed));
    Assert.That(result.ErrorMessage, Does.Not.Contain("hunter2"));
  }

  [Test]
  [Category("Regression")]
  public async Task HandlerExceptionDetailsCanBeIncludedAndAlwaysReachTheObserver()
  {
    var observed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          IncludeExceptionDetails = true,
          Observer = new HandlerFailedObserver(exception => observed.TrySetResult(exception))
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, _, _) => throw new InvalidOperationException("detailed failure")
        });
    await using var client = await ConnectAsync(listener);

    var result = await client.RequestAsync("test", "op", [], TimeSpan.FromSeconds(5));
    var exception = await observed.Task.WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(result.ErrorMessage, Does.Contain("detailed failure"));
    Assert.That(exception, Is.TypeOf<InvalidOperationException>());
  }

  [Test]
  [Category("Regression")]
  public async Task HandlerSocketProtocolExceptionKeepsItsErrorCode()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, _, _) => throw new SocketProtocolException("not_found", "No such key.")
        });
    await using var client = await ConnectAsync(listener);

    var result = await client.RequestAsync("test", "op", [], TimeSpan.FromSeconds(5));

    Assert.That(result.ErrorCode, Is.EqualTo("not_found"));
    Assert.That(result.ErrorMessage, Is.EqualTo("No such key."));
  }

  [Test]
  [Category("Regression")]
  public async Task UnauthorizedEventCanBeDroppedWithoutClosing()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          CloseOnUnauthorizedEvent = false,
          Authorizer = new DelegateSocketAuthorizer
          {
            OnEvent = (_, _, _) => ValueTask.FromResult(SocketAuthorizationResult.Denied("no"))
          }
        },
        new DelegateSocketPeerHandler { OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success()) });
    await using var client = await ConnectAsync(listener);

    await client.SendEventAsync("test", "denied", [1]);
    var result = await client.RequestAsync("test", "ok", [], TimeSpan.FromSeconds(5));

    Assert.That(result.Succeeded, Is.True);
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
  }

  [Test]
  [Category("Regression")]
  public async Task StreamFailsWhenProducerStallsLongerThanIdleTimeout()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", StreamIdleTimeout = TimeSpan.FromMilliseconds(200) },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(
              new SlowChunkStream(chunks: 3, chunkBytes: 1024, delay: TimeSpan.FromSeconds(2))))
        });
    await using var client = await ConnectAsync(listener);

    await using var stream = await client.OpenStreamAsync("test", "stalled", [], TimeSpan.FromSeconds(5));
    var exception = await Assert.ThrowsAsync<SocketProtocolException>(async () => await CountBytesAsync(stream));

    Assert.That(exception.ErrorCode, Is.EqualTo(SocketProtocolError.Timeout));
  }

  [Test]
  [Category("Regression")]
  public async Task Bug15_CompressedResponsesAndStreamsRoundTrip()
  {
    var text = System.Text.Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("compressible payload ", 8192)));
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", PayloadCompression = SocketPayloadCompression.Brotli },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload }),
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new MemoryStream(text)))
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions
    {
      NodeId = "client",
      PayloadCompression = SocketPayloadCompression.Brotli
    });

    var echo = await client.RequestAsync("test", "echo", text, TimeSpan.FromSeconds(5));
    await using var stream = await client.OpenStreamAsync("test", "text", [], TimeSpan.FromSeconds(5));
    using var copy = new MemoryStream();
    await stream.CopyToAsync(copy);

    Assert.That(echo.Succeeded, Is.True, echo.ErrorMessage);
    Assert.That(echo.Payload.ToArray(), Is.EqualTo(text));
    Assert.That(copy.ToArray(), Is.EqualTo(text));
  }

  [Test]
  [Category("Regression")]
  public async Task DisposingPeerCompletesInFlightRequestsWithConnectionClosed()
  {
    var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, _, cancellationToken) =>
          {
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return SocketResponse.Success();
          }
        });
    var client = await ConnectAsync(listener);

    var inFlight = client.RequestAsync("test", "hang", [], TimeSpan.FromSeconds(10)).AsTask();
    await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
    await client.DisposeAsync();
    var result = await inFlight.WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.ConnectionClosed));
  }

  [Test]
  [Category("Regression")]
  public async Task DisposeDoesNotHangWhenRemoteStopsReading()
  {
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    try
    {
      var accept = Task.Run(async () =>
      {
        var server = await listener.AcceptTcpClientAsync();
        await RunRawNoAuthServerHandshakeAsync(server.GetStream());
        return server;
      });
      var client = await new SocketConnector(new SocketFabricOptions
      {
        NodeId = "client",
        MaxFrameSizeBytes = 64 * 1024 * 1024
      }).ConnectAsync((IPEndPoint)listener.LocalEndpoint, new DelegateSocketPeerHandler());
      using var server = await accept;

      // The server never reads again, so this write fills the socket buffers and blocks while holding the write lock.
      _ = client.RequestAsync("test", "large", new byte[48 * 1024 * 1024], TimeSpan.FromSeconds(30)).AsTask();
      await Task.Delay(300);

      await client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
      Assert.That(client.State, Is.EqualTo(SocketPeerState.Closed));
    }
    finally
    {
      listener.Stop();
    }
  }

  [Test]
  [Category("Regression")]
  public async Task DisposingStreamEarlyKeepsConnectionUsable()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(64L * 1024 * 1024))),
          OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success())
        });
    await using var client = await ConnectAsync(listener);

    for (var i = 0; i < 5; i++)
    {
      var stream = await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(5));
      await stream.ReadAtLeastAsync(new byte[1024], 1);
      await stream.DisposeAsync();
    }

    await Task.Delay(200);
    var result = await client.RequestAsync("test", "ok", [], TimeSpan.FromSeconds(5));

    Assert.That(result.Succeeded, Is.True, $"connection was lost: {result.ErrorCode}");
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
  }

  // Stress tests for defects suspected from code reading. They may pass today because the races are
  // narrow; they exist so the Phase 2 rework is checked against them.

  [Test]
  [Category("Stress")]
  public async Task Stress_LateResponsesNeverCompleteADifferentRequest()
  {
    var disconnects = new ConcurrentQueue<string>();
    var observer = new DisconnectObserver((side, exception) => disconnects.Enqueue($"{side}: {exception}"));
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 64, Observer = observer },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, request, _) =>
          {
            await Task.Delay(Random.Shared.Next(0, 20));
            return new SocketResponse { Succeeded = true, Payload = request.Payload };
          }
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", Observer = observer });
    var mismatches = 0;
    var succeeded = 0;

    var run = Parallel.ForEachAsync(
        Enumerable.Range(0, 3000),
        new ParallelOptions { MaxDegreeOfParallelism = 64 },
        async (index, _) =>
        {
          var payload = BitConverter.GetBytes(index);
          var result = await client.RequestAsync(
              "test",
              "echo",
              payload,
              TimeSpan.FromMilliseconds(Random.Shared.Next(1, 25)));
          if (!result.Succeeded)
            return;

          Interlocked.Increment(ref succeeded);
          if (!result.Payload.Span.SequenceEqual(payload))
            Interlocked.Increment(ref mismatches);
        });
    try
    {
      await run;
    }
    catch (Exception ex)
    {
      Assert.Fail($"{ex.GetType().Name}: {ex.Message}{Environment.NewLine}Disconnects:{Environment.NewLine}{string.Join(Environment.NewLine, disconnects)}");
    }

    Assert.That(disconnects, Is.Empty, "a peer disconnected during the run");
    Assert.That(mismatches, Is.Zero);
    Assert.That(succeeded, Is.GreaterThan(0));
  }

  [Test]
  [Category("Regression")]
  [Ignore(Phase2EventOrdering + " Confirmed in Phase 0.")]
  public async Task Stress_EventsFromOnePeerAreHandledInOrder()
  {
    const int events = 2000;
    var seen = new ConcurrentQueue<int>();
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, socketEvent, _) =>
          {
            seen.Enqueue(BinaryPrimitives.ReadInt32BigEndian(socketEvent.Payload.Span));
            return ValueTask.CompletedTask;
          }
        });
    await using var client = await ConnectAsync(listener);

    var payload = new byte[4];
    for (var i = 0; i < events; i++)
    {
      BinaryPrimitives.WriteInt32BigEndian(payload, i);
      await client.SendEventAsync("test", "ordered", payload.ToArray());
    }
    await WaitUntilAsync(() => seen.Count == events, TimeSpan.FromSeconds(10));

    Assert.That(seen, Is.Ordered);
  }

  [Test]
  [Category("Stress")]
  public async Task Stress_CancellingALargeWriteDoesNotCorruptTheConnection()
  {
    var options = new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 64 * 1024 * 1024,
      MaxDecompressedPayloadSizeBytes = 64 * 1024 * 1024
    };
    await using var listener = await StartListenerAsync(
        options,
        new DelegateSocketPeerHandler { OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success()) });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions
    {
      NodeId = "client",
      MaxFrameSizeBytes = 64 * 1024 * 1024,
      MaxDecompressedPayloadSizeBytes = 64 * 1024 * 1024
    });
    var large = new byte[48 * 1024 * 1024];

    for (var attempt = 0; attempt < 5; attempt++)
    {
      using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(2 + attempt));
      await client.RequestAsync("test", "large", large, TimeSpan.FromSeconds(10), cancellationToken: cancellation.Token);
      var check = await client.RequestAsync("test", "small", [1], TimeSpan.FromSeconds(10));
      Assert.That(check.Succeeded, Is.True, $"attempt {attempt}: connection unusable after cancelled write: {check.ErrorCode}");
    }

    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
  }

  [Test]
  [Category("Stress")]
  public async Task Stress_StreamDataStaysIntactWhenConnectionDropsMidRead()
  {
    for (var iteration = 0; iteration < 10; iteration++)
    {
      SocketPeer serverPeer = null;
      await using var listener = await StartListenerAsync(
          new SocketFabricOptions { NodeId = "server" },
          new DelegateSocketPeerHandler
          {
            OnStream = (peer, _, _) =>
            {
              serverPeer = peer;
              return ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(64L * 1024 * 1024)));
            }
          });
      await using var client = await ConnectAsync(listener);
      await using var stream = await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(5));

      var buffer = new byte[7919];
      long position = 0;
      try
      {
        while (true)
        {
          var read = await stream.ReadAsync(buffer);
          if (read == 0)
            break;

          for (var i = 0; i < read; i++)
          {
            if (buffer[i] != PatternStream.ByteAt(position + i))
              Assert.Fail($"iteration {iteration}: corrupted byte at offset {position + i}");
          }

          position += read;
          if (position > 1024 * 1024 && serverPeer is not null)
          {
            _ = serverPeer.DisposeAsync().AsTask();
            serverPeer = null;
          }
        }
      }
      catch (SocketProtocolException)
      {
      }
    }
  }

  [Test]
  [Category("Stress")]
  [Ignore(Phase2FlowControl + " Confirmed in Phase 0: both peers block writing while neither read loop drains.")]
  public async Task Stress_BidirectionalFloodDoesNotDeadlock()
  {
    SocketPeer serverPeer = null;
    var serverConnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var handler = new DelegateSocketPeerHandler
    {
      OnConnect = (peer, _) =>
      {
        serverPeer = peer;
        serverConnected.TrySetResult();
        return ValueTask.CompletedTask;
      },
      OnRequest = async (_, request, cancellationToken) =>
      {
        await Task.Delay(5, cancellationToken);
        return new SocketResponse { Succeeded = true, Payload = request.Payload };
      }
    };
    var options = new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1, MaxQueuedHandlers = 1 };
    await using var listener = await StartListenerAsync(options, handler);
    await using var client = await new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      MaxConcurrentHandlers = 1,
      MaxQueuedHandlers = 1
    }).ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler { OnRequest = handler.OnRequest });
    await serverConnected.Task.WaitAsync(TimeSpan.FromSeconds(5));

    var payload = new byte[512 * 1024];
    var fromClient = Enumerable.Range(0, 64).Select(_ => client.RequestAsync("test", "flood", payload, TimeSpan.FromSeconds(20)).AsTask());
    var fromServer = Enumerable.Range(0, 64).Select(_ => serverPeer.RequestAsync("test", "flood", payload, TimeSpan.FromSeconds(20)).AsTask());

    var results = await Task.WhenAll(fromClient.Concat(fromServer)).WaitAsync(TimeSpan.FromSeconds(30));

    Assert.That(results.Select(result => result.ErrorCode), Has.None.EqualTo(SocketProtocolError.Timeout));
  }

  static async ValueTask<SocketListener> StartListenerAsync(SocketFabricOptions options, DelegateSocketPeerHandler handler)
  {
    var listener = new SocketListener(options);
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), handler);
    return listener;
  }

  static async ValueTask<SocketPeer> ConnectAsync(SocketListener listener, SocketFabricOptions options = null)
  {
    var connector = new SocketConnector(options ?? new SocketFabricOptions { NodeId = "client" });
    return await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
  }

  static async ValueTask<string> TryOpenStreamAsync(SocketPeer client)
  {
    try
    {
      await using var stream = await client.OpenStreamAsync("test", "stream", [], TimeSpan.FromSeconds(2));
      await CountBytesAsync(stream);
      return null;
    }
    catch (SocketProtocolException ex)
    {
      return ex.ErrorCode;
    }
  }

  static async ValueTask<long> CountBytesAsync(Stream stream)
  {
    var buffer = new byte[16 * 1024];
    long total = 0;
    int read;
    while ((read = await stream.ReadAsync(buffer)) > 0)
      total += read;
    return total;
  }

  // The server half of a no-auth handshake, for tests that need a remote that misbehaves afterwards.
  static async ValueTask RunRawNoAuthServerHandshakeAsync(Stream stream)
  {
    var reader = new SocketFrameReader(stream, 1024 * 1024);
    var writer = new SocketFrameWriter(stream, 1024 * 1024);
    var hello = await reader.ReadAsync(CancellationToken.None);
    Assert.That(hello.Operation, Is.EqualTo("hello"));
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "challenge",
      Ok = true,
      Payload = Serialization.JsonSocketMessageCodec.Shared.Serialize(new SocketHandshakeChallenge(
          SocketFabricOptions.CurrentProtocolVersion,
          "raw-server",
          Guid.NewGuid().ToString("N"),
          string.Empty))
    }, CancellationToken.None);
    var proof = await reader.ReadAsync(CancellationToken.None);
    Assert.That(proof.Operation, Is.EqualTo("proof"));
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "ready",
      Ok = true
    }, CancellationToken.None);
  }

  sealed class DisconnectObserver(Action<string, Exception> onDisconnected) : Observability.ISocketObserver
  {
    public void PeerDisconnected(SocketPeer peer, Exception exception)
    {
      onDisconnected(peer.LocalNodeId, exception);
    }
  }

  sealed class HandlerFailedObserver(Action<Exception> onHandlerFailed) : Observability.ISocketObserver
  {
    public void HandlerFailed(SocketPeer peer, string service, string operation, Exception exception)
    {
      onHandlerFailed(exception);
    }
  }

  static async ValueTask WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
  {
    var until = DateTime.UtcNow + timeout;
    while (!condition() && DateTime.UtcNow < until)
      await Task.Delay(10);
  }

  sealed class SlowReadStreamFilter(TimeSpan delay) : ISocketStreamFilter
  {
    public ValueTask<Stream> WrapClientAsync(Stream stream, string localNodeId, CancellationToken cancellationToken)
    {
      return ValueTask.FromResult<Stream>(new SlowReadStream(stream, delay));
    }

    public ValueTask<Stream> WrapServerAsync(Stream stream, string localNodeId, CancellationToken cancellationToken)
    {
      return ValueTask.FromResult(stream);
    }
  }

  // Delays every read so data the server sends back-to-back accumulates in the socket buffer,
  // which is what a busy client or a real network looks like.
  sealed class SlowReadStream(Stream inner, TimeSpan delay) : Stream
  {
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
      get => throw new NotSupportedException();
      set => throw new NotSupportedException();
    }

    public override void Flush()
    {
      inner.Flush();
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
      return inner.FlushAsync(cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
      return inner.Read(buffer, offset, count);
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
      await Task.Delay(delay, cancellationToken);
      return await inner.ReadAsync(buffer, cancellationToken);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
      inner.Write(buffer, offset, count);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
      return inner.WriteAsync(buffer, cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
      throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
      throw new NotSupportedException();
    }

    protected override void Dispose(bool disposing)
    {
      if (disposing)
        inner.Dispose();
      base.Dispose(disposing);
    }
  }

  sealed class SlowChunkStream(int chunks, int chunkBytes, TimeSpan delay) : Stream
  {
    int Remaining = chunks;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
      get => throw new NotSupportedException();
      set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
      throw new NotSupportedException();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
      if (Remaining-- <= 0)
        return 0;

      await Task.Delay(delay, cancellationToken);
      var count = Math.Min(chunkBytes, buffer.Length);
      buffer.Span.Slice(0, count).Fill(1);
      return count;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
      throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
      throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
      throw new NotSupportedException();
    }
  }

  sealed class PatternStream(long length) : Stream
  {
    long PositionValue;

    public static byte ByteAt(long position)
    {
      return (byte)(position % 251);
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => length;

    public override long Position
    {
      get => PositionValue;
      set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
      return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
      var count = (int)Math.Min(buffer.Length, length - PositionValue);
      for (var i = 0; i < count; i++)
        buffer[i] = ByteAt(PositionValue + i);
      PositionValue += count;
      return count;
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
      return ValueTask.FromResult(Read(buffer.Span));
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
      throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
      throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
      throw new NotSupportedException();
    }
  }
}
