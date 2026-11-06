using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Fabric;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;
using PureGate.Socket.Security;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Regression tests: each one reproduces a specific defect and fails without its fix.
// Each test names the bug number it guards.
public sealed partial class SocketFabricTests
{

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
  public async Task Bug01_StreamEventAndBatchHandlerOperationCanceled_KeepWorkerAlive()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1 },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => throw new OperationCanceledException("not ours"),
          OnEvent = (_, _, _) => throw new OperationCanceledException("not ours"),
          OnRequest = (_, request, _) => request.Operation == "boom"
              ? throw new OperationCanceledException("not ours")
              : ValueTask.FromResult(SocketResponse.Success())
        });
    await using var client = await ConnectAsync(listener);

    var batch = await client.RequestBatchAsync([new SocketBatchItem(1, "test", "boom", [])], TimeSpan.FromSeconds(2));
    var streamError = await TryOpenStreamAsync(client);
    await client.SendEventAsync("test", "event", []);
    var next = await client.RequestAsync("test", "ok", [], TimeSpan.FromSeconds(2));

    Assert.That(batch[0].ErrorCode, Is.EqualTo(SocketProtocolError.HandlerFailed));
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
  public async Task Bug02_CompressionAndChunkingPreserveEveryHeaderField()
  {
    var seen = new ConcurrentQueue<(string Service, string Operation, string IdempotencyKey, int Length)>();
    var options = new SocketFabricOptions
    {
      NodeId = "server",
      PayloadCompression = SocketPayloadCompression.Brotli,
      MaxFrameSizeBytes = 256 * 1024
    };
    await using var listener = await StartListenerAsync(
        options,
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) =>
          {
            seen.Enqueue((request.Service, request.Operation, request.IdempotencyKey, request.Payload.Length));
            return ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload });
          }
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions
    {
      NodeId = "client",
      PayloadCompression = SocketPayloadCompression.Brotli,
      MaxFrameSizeBytes = 256 * 1024
    });
    var compressible = new byte[64 * 1024];
    var largeCompressible = new byte[4 * 1024 * 1024];
    var largeRandom = new byte[1024 * 1024];
    Random.Shared.NextBytes(largeRandom);

    // Compressed into one frame (64 KB and 4 MB of zeros, which compress far below the 256 KB frame
    // limit), and incompressible data larger than a frame, which is chunked.
    var small = await client.RequestAsync("svc-a", "op-1", compressible, TimeSpan.FromSeconds(5), idempotencyKey: "key-1");
    var large = await client.RequestAsync("svc-a", "op-1", largeCompressible, TimeSpan.FromSeconds(5));
    var random = await client.RequestAsync("svc-b", "op-2", largeRandom, TimeSpan.FromSeconds(5), idempotencyKey: "key-2");
    var batch = await client.RequestBatchAsync([new SocketBatchItem(1, "svc-c", "op-3", compressible)], TimeSpan.FromSeconds(5));

    Assert.That(small.Payload.ToArray(), Is.EqualTo(compressible));
    Assert.That(large.Payload.ToArray(), Is.EqualTo(largeCompressible));
    Assert.That(random.Payload.ToArray(), Is.EqualTo(largeRandom));
    Assert.That(batch[0].Payload, Is.EqualTo(compressible));
    Assert.That(seen, Is.EqualTo(new[]
    {
      ("svc-a", "op-1", "key-1", compressible.Length),
      ("svc-a", "op-1", "", largeCompressible.Length),
      ("svc-b", "op-2", "key-2", largeRandom.Length),
      ("svc-c", "op-3", "", compressible.Length)
    }));
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
  public async Task LocalInFlightLimitIsOverloadedButFullRemoteQueueMakesSendersWait()
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

    // The local in-flight limit still fails fast. A full remote queue no longer rejects anything: senders
    // wait for message credit, so every flood request completes once a handler slot frees up.
    Assert.That(rejectedLocally.ErrorCode, Is.EqualTo(SocketProtocolError.Overloaded));
    Assert.That(flood.Select(result => result.Succeeded), Has.All.True, string.Join(", ", flood.Select(result => result.ErrorCode)));
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

  // Chunk expiry, chunked-write cancellation, batch error handling and observer reporting.

  [Test]
  [Category("Regression")]
  public async Task ChunkedMessageThatKeepsProgressingOutlivesChunkTimeout()
  {
    var delivered = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", ChunkTimeout = TimeSpan.FromMilliseconds(600) },
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, socketEvent, _) =>
          {
            delivered.TrySetResult(socketEvent.Payload.Length);
            return ValueTask.CompletedTask;
          }
        });
    using var tcp = new TcpClient(AddressFamily.InterNetwork);
    await tcp.ConnectAsync(listener.EndPoint);
    await RunRawNoAuthHandshakeAsync(tcp.GetStream());
    var writer = new SocketFrameWriter(tcp.GetStream(), 1024 * 1024);

    // An 800-byte event split into 8 chunks sent 300 ms apart: 2.4 s in total, well past ChunkTimeout,
    // but never 600 ms without progress.
    var body = EncodeBody(RawEvent(1, "chunked", new byte[800]));
    var chunks = SplitIntoChunks(1, SocketMessageKind.Event, body, chunkCount: 8);
    foreach (var chunk in chunks)
    {
      await writer.WriteAsync(chunk, CancellationToken.None);
      await Task.Delay(300);
    }

    var length = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.That(length, Is.EqualTo(800));
  }

  [Test]
  [Category("Regression")]
  public async Task LateChunkOfExpiredMessageIsDroppedWithoutClosingConnection()
  {
    var events = new ConcurrentQueue<string>();
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", ChunkTimeout = TimeSpan.FromMilliseconds(300) },
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, socketEvent, _) =>
          {
            events.Enqueue(socketEvent.Operation);
            return ValueTask.CompletedTask;
          }
        });
    using var tcp = new TcpClient(AddressFamily.InterNetwork);
    await tcp.ConnectAsync(listener.EndPoint);
    await RunRawNoAuthHandshakeAsync(tcp.GetStream());
    var writer = new SocketFrameWriter(tcp.GetStream(), 1024 * 1024);

    await writer.WriteAsync(RawChunk(1, 0, 2, 200, new byte[100]), CancellationToken.None);
    await Task.Delay(1300);
    await writer.WriteAsync(RawChunk(1, 1, 2, 200, new byte[100]), CancellationToken.None);
    await writer.WriteAsync(RawEvent(2, "after"), CancellationToken.None);
    await WaitUntilAsync(() => !events.IsEmpty, TimeSpan.FromSeconds(5));

    Assert.That(events, Is.EqualTo(new[] { "after" }), "the expired message must be dropped and the connection must stay open");
  }

  [Test]
  [Category("Regression")]
  public async Task CancelledChunkedWriteNeverReadsTheCallersBufferAfterReturning()
  {
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    try
    {
      var server = Task.Run(async () =>
      {
        using var socket = await listener.AcceptTcpClientAsync();
        var reader = await RunRawNoAuthServerHandshakeAsync(socket.GetStream());
        await resume.Task;
        int chunks = 0, corrupted = 0;
        while (true)
        {
          using var idle = new CancellationTokenSource(TimeSpan.FromSeconds(1.5));
          SocketFrame frame;
          try
          {
            frame = await reader.ReadAsync(idle.Token);
          }
          catch (Exception)
          {
            break;
          }

          if (frame is null)
            break;
          if (frame.Kind != SocketMessageKind.Chunk)
            continue;

          // Chunk 0 starts with the encoded frame header and is sent before the caller gives up; every
          // later chunk is pure payload, so any byte other than 1 there came from the reused buffer.
          chunks++;
          if (frame.ChunkIndex > 0 && frame.PayloadMemory.Span.IndexOfAnyExcept((byte)1) >= 0)
            corrupted++;
        }

        return (chunks, corrupted);
      });

      await using var client = await new SocketConnector(new SocketFabricOptions { NodeId = "client" })
          .ConnectAsync((IPEndPoint)listener.LocalEndpoint, new DelegateSocketPeerHandler());
      var payload = new byte[64 * 1024 * 1024];
      payload.AsSpan().Fill(1);

      // The server is not reading, so the chunked write blocks; the caller then gives up.
      using (var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(300)))
      {
        try
        {
          await client.SendEventAsync("test", "big", payload, cancellation.Token);
          Assert.Inconclusive("The write finished before the cancellation; socket buffers were large enough to hold it.");
        }
        catch (OperationCanceledException)
        {
        }
      }

      // The caller owns its buffer again and reuses it. Nothing sent from here on may contain these bytes.
      payload.AsSpan().Fill(0xEE);
      resume.TrySetResult();
      var (chunks, corrupted) = await server.WaitAsync(TimeSpan.FromSeconds(30));

      Assert.That(corrupted, Is.Zero, "a chunk was copied from the caller's buffer after the caller returned");
      Assert.That(chunks, Is.GreaterThan(0).And.LessThan(256), "the chunked write should stop at the next chunk boundary");
    }
    finally
    {
      listener.Stop();
    }
  }

  [Test]
  [Category("Regression")]
  public async Task HandlerProtocolErrorInsideEventBatchDoesNotCloseConnection()
  {
    var handled = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, _, _) =>
          {
            Interlocked.Increment(ref handled);
            throw new SocketProtocolException(SocketProtocolError.ProtocolError, "bad input");
          },
          OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success())
        });
    await using var client = await ConnectAsync(listener);

    await client.SendEventBatchAsync([new SocketBatchItem(1, "test", "event", [1]), new SocketBatchItem(2, "test", "event", [2])]);
    // Both items must run (the batch continues past a failing item), and any close must have happened
    // before the connection is checked.
    await WaitUntilAsync(() => Volatile.Read(ref handled) == 2, TimeSpan.FromSeconds(5));
    await Task.Delay(200);
    Assert.That(handled, Is.EqualTo(2), "the batch stopped at the first failing item");
    var result = await client.RequestAsync("test", "ok", [], TimeSpan.FromSeconds(5));

    Assert.That(result.Succeeded, Is.True, $"connection was closed: {result.ErrorCode}");
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
  }

  [Test]
  [Category("Regression")]
  public async Task StreamDisposedByClientIsObservedAsCancelledNotTimeout()
  {
    var completed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          Observer = new DelegateSocketObserver
          {
            OnRequestCompleted = (_, _, operation, _, errorCode) =>
            {
              if (operation == "pattern")
                completed.TrySetResult(errorCode);
            }
          }
        },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(64L * 1024 * 1024)))
        });
    await using var client = await ConnectAsync(listener);

    var stream = await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(5));
    await stream.ReadAtLeastAsync(new byte[1024], 1);
    await stream.DisposeAsync();
    var errorCode = await completed.Task.WaitAsync(TimeSpan.FromSeconds(10));

    Assert.That(errorCode, Is.EqualTo(SocketProtocolError.Cancelled));
  }

  [Test]
  [Category("Regression")]
  public async Task HandlerFailuresAreReportedUnderTheirOperation()
  {
    var observer = new HandlerFailedNamesObserver();
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", Observer = observer },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, _, _) => throw new InvalidOperationException("failure")
        });
    await using var client = await ConnectAsync(listener);

    await client.RequestAsync("test", "single", [], TimeSpan.FromSeconds(5));
    await client.RequestBatchAsync([new SocketBatchItem(1, "test", "batched", [])], TimeSpan.FromSeconds(5));
    await WaitUntilAsync(() => observer.Operations.Count >= 2, TimeSpan.FromSeconds(5));

    Assert.That(observer.Operations, Is.EquivalentTo(new[] { "single", "batched" }));
  }

  [Test]
  [Category("Regression")]
  public async Task FramesRoundTripThroughTheCodecWhetherContiguousOrSegmented()
  {
    var frames = new[]
    {
      new SocketFrame
      {
        Kind = SocketMessageKind.Request, MessageId = 42, DeadlineMs = 1500,
        Service = "svc-ü", Operation = "op", IdempotencyKey = "key", Payload = Enumerable.Range(0, 70_000).Select(i => (byte)i).ToArray()
      },
      new SocketFrame { Kind = SocketMessageKind.Request, MessageId = 43, Service = "svc-ü", Operation = "op", Payload = [1, 2, 3] },
      new SocketFrame
      {
        Kind = SocketMessageKind.Chunk, OriginalKind = SocketMessageKind.Event, MessageId = 7,
        ChunkIndex = 1, ChunkCount = 3, TotalPayloadLength = 300, Payload = new byte[100]
      },
      new SocketFrame { Kind = SocketMessageKind.Ping, MessageId = -1 },
      new SocketFrame { Kind = SocketMessageKind.Credit, MessageId = 0, Value = 65536 },
      new SocketFrame { Kind = SocketMessageKind.Response, MessageId = 9, Ok = false, ErrorCode = SocketProtocolError.Timeout, ErrorMessage = "late" },
      new SocketFrame { Kind = SocketMessageKind.Response, MessageId = 10, Ok = false, ErrorCode = "not_found", ErrorMessage = "no such key" },
      new SocketFrame { Kind = SocketMessageKind.Response, MessageId = 11, Ok = true, Payload = [5] },
      new SocketFrame { Kind = SocketMessageKind.StreamItem, MessageId = 12, Payload = new byte[5000] }
    };
    using var encoded = new MemoryStream();
    var writer = new SocketFrameWriter(encoded, 1024 * 1024, useMethodTable: true);
    foreach (var frame in frames)
      await writer.WriteAsync(frame, CancellationToken.None);

    var contiguous = await ReadAllFramesAsync(new MemoryStream(encoded.ToArray()), frames.Length);
    var segmented = await ReadAllFramesAsync(new TrickleStream(encoded.ToArray(), maxBytesPerRead: 7), frames.Length);

    for (var i = 0; i < frames.Length; i++)
    {
      foreach (var actual in new[] { contiguous[i], segmented[i] })
      {
        var expected = frames[i];
        expected.Service ??= string.Empty;
        expected.Operation ??= string.Empty;
        expected.IdempotencyKey ??= string.Empty;
        AssertSameFrame(actual, expected);
      }
    }
  }

  static void AssertSameFrame(SocketFrame actual, SocketFrame expected)
  {
      Assert.Multiple(() =>
      {
        Assert.That(actual.Kind, Is.EqualTo(expected.Kind));
        Assert.That(actual.MessageId, Is.EqualTo(expected.MessageId));
        Assert.That(actual.Value, Is.EqualTo(expected.Value));
        Assert.That(actual.DeadlineMs, Is.EqualTo(expected.DeadlineMs));
        Assert.That(actual.OriginalKind, Is.EqualTo(expected.OriginalKind));
        Assert.That(actual.ChunkIndex, Is.EqualTo(expected.ChunkIndex));
        Assert.That(actual.ChunkCount, Is.EqualTo(expected.ChunkCount));
        Assert.That(actual.TotalPayloadLength, Is.EqualTo(expected.TotalPayloadLength));
        Assert.That(actual.Ok, Is.EqualTo(expected.Ok));
        Assert.That(actual.Service, Is.EqualTo(expected.Service));
        Assert.That(actual.Operation, Is.EqualTo(expected.Operation));
        Assert.That(actual.IdempotencyKey, Is.EqualTo(expected.IdempotencyKey));
        Assert.That(actual.ErrorCode, Is.EqualTo(expected.ErrorCode));
        Assert.That(actual.ErrorMessage, Is.EqualTo(expected.ErrorMessage));
        Assert.That(actual.Payload, Is.EqualTo(expected.Payload));
      });
  }

  [Test]
  [Category("Protocol")]
  public async Task RepeatedMethodsCostOnlyAFewHeaderBytes()
  {
    using var encoded = new MemoryStream();
    var writer = new SocketFrameWriter(encoded, 1024 * 1024, useMethodTable: true);
    var frame = new SocketFrame { Kind = SocketMessageKind.Request, MessageId = 1, Service = "inventory", Operation = "reserve", DeadlineMs = 10_000, Payload = new byte[64] };

    await writer.WriteAsync(frame, CancellationToken.None);
    var first = encoded.Length;
    frame.MessageId = 2;
    await writer.WriteAsync(frame, CancellationToken.None);
    var second = encoded.Length - first;

    // Length prefix (4) + kind, flags, id and method number (1 byte each) and deadline (2) + payload.
    Assert.That(second, Is.EqualTo(4 + 6 + 64));
    Assert.That(first, Is.GreaterThan(second), "the first use carries the method's definition");
  }

  [Test]
  [Category("Protocol")]
  public async Task MethodsBeyondTheTableLimitAreSentByName()
  {
    var seen = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) =>
          {
            Interlocked.Increment(ref seen);
            return ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = System.Text.Encoding.UTF8.GetBytes(request.Operation) });
          }
        });
    await using var client = await ConnectAsync(listener);

    const int methods = 4096 + 50;
    var mismatches = 0;
    for (var i = 0; i < methods; i++)
    {
      var operation = $"op-{i}";
      var result = await client.RequestAsync("test", operation, [], TimeSpan.FromSeconds(5));
      if (!result.Succeeded || System.Text.Encoding.UTF8.GetString(result.Payload.Span) != operation)
        mismatches++;
    }

    // Requests for methods defined early still resolve correctly after the table filled up.
    var early = await client.RequestAsync("test", "op-0", [], TimeSpan.FromSeconds(5));

    Assert.That(mismatches, Is.Zero);
    Assert.That(System.Text.Encoding.UTF8.GetString(early.Payload.Span), Is.EqualTo("op-0"));
    Assert.That(seen, Is.EqualTo(methods + 1));
  }

  static async ValueTask<List<SocketFrame>> ReadAllFramesAsync(Stream stream, int count)
  {
    var reader = new SocketFrameReader(stream, 1024 * 1024);
    var frames = new List<SocketFrame>();
    for (var i = 0; i < count; i++)
      frames.Add(await reader.ReadAsync(CancellationToken.None));
    return frames;
  }

  // Returns at most a few bytes per read, so the pipe holds frames in many small segments.
  sealed class TrickleStream(byte[] data, int maxBytesPerRead) : Stream
  {
    int PositionValue;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => data.Length;

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
      var count = Math.Min(Math.Min(buffer.Length, maxBytesPerRead), data.Length - PositionValue);
      data.AsSpan(PositionValue, count).CopyTo(buffer);
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

  // Liveness.

  [Test]
  [Category("Regression")]
  public async Task HalfOpenConnectionIsClosedSoonAfterARequestTimesOut()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler { OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success()) });
    await using var proxy = FreezableProxy.Start(listener.EndPoint);
    await using var client = await new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      LivenessProbeTimeout = TimeSpan.FromMilliseconds(300)
    }).ConnectAsync(proxy.EndPoint, new DelegateSocketPeerHandler());

    proxy.FreezeExisting();
    var result = await client.RequestAsync("test", "op", [], TimeSpan.FromMilliseconds(300));
    await client.Completion.WaitAsync(TimeSpan.FromSeconds(3));

    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.Timeout));
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Closed));
  }

  [Test]
  [Category("Regression")]
  public async Task ClientRetriesOnAFreshConnectionWhenTheOldOneIsHalfOpen()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler { OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success()) });
    await using var proxy = FreezableProxy.Start(listener.EndPoint);
    await using var client = new SocketClient(
        proxy.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions
        {
          NodeId = "client",
          RequestTimeout = TimeSpan.FromMilliseconds(300),
          LivenessProbeTimeout = TimeSpan.FromMilliseconds(300)
        });
    var first = await client.GetPeerAsync();

    proxy.FreezeExisting();
    var result = await client.RequestAsync("test", "op", 1, new SocketClientRequestOptions
    {
      Idempotent = true,
      MaxAttempts = 2,
      RetryDelay = TimeSpan.Zero
    }).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
    var second = await client.GetPeerAsync();

    Assert.That(result.Succeeded, Is.True, $"retry failed: {result.ErrorCode}");
    Assert.That(second, Is.Not.SameAs(first));
    Assert.That(first.State, Is.EqualTo(SocketPeerState.Closed));
  }

  [Test]
  [Category("Regression")]
  public async Task SlowButAliveRemoteIsNotClosedByTheLivenessCheck()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, _, cancellationToken) =>
          {
            await Task.Delay(1000, cancellationToken);
            return SocketResponse.Success();
          }
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions
    {
      NodeId = "client",
      LivenessProbeTimeout = TimeSpan.FromMilliseconds(300)
    });

    var result = await client.RequestAsync("test", "slow", [], TimeSpan.FromMilliseconds(200));
    await Task.Delay(800);
    var next = await client.RequestAsync("test", "slow", [], TimeSpan.FromSeconds(5));

    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.Timeout));
    Assert.That(client.State, Is.EqualTo(SocketPeerState.Connected));
    Assert.That(next.Succeeded, Is.True);
  }

  // A TCP proxy whose current connections can be frozen: they stay open but forward nothing, exactly like a
  // link whose remote vanished. Connections made after a freeze forward normally.
  sealed class FreezableProxy : IAsyncDisposable
  {
    readonly TcpListener Listener;
    readonly IPEndPoint Upstream;
    readonly ConcurrentBag<ProxiedConnection> Connections = [];
    readonly CancellationTokenSource Lifetime = new();
    Task AcceptLoop;

    FreezableProxy(IPEndPoint upstream)
    {
      Upstream = upstream;
      Listener = new TcpListener(IPAddress.Loopback, 0);
    }

    public IPEndPoint EndPoint => (IPEndPoint)Listener.LocalEndpoint;

    public static FreezableProxy Start(IPEndPoint upstream)
    {
      var proxy = new FreezableProxy(upstream);
      proxy.Listener.Start();
      proxy.AcceptLoop = Task.Run(proxy.AcceptAsync);
      return proxy;
    }

    public void FreezeExisting()
    {
      foreach (var connection in Connections)
        connection.Frozen = true;
    }

    async Task AcceptAsync()
    {
      try
      {
        while (true)
        {
          var downstream = await Listener.AcceptTcpClientAsync(Lifetime.Token);
          var upstream = new TcpClient(AddressFamily.InterNetwork) { NoDelay = true };
          await upstream.ConnectAsync(Upstream, Lifetime.Token);
          var connection = new ProxiedConnection(downstream, upstream);
          Connections.Add(connection);
          _ = PumpAsync(connection, downstream.GetStream(), upstream.GetStream());
          _ = PumpAsync(connection, upstream.GetStream(), downstream.GetStream());
        }
      }
      catch
      {
      }
    }

    async Task PumpAsync(ProxiedConnection connection, NetworkStream from, NetworkStream to)
    {
      var buffer = new byte[64 * 1024];
      try
      {
        while (true)
        {
          var read = await from.ReadAsync(buffer, Lifetime.Token);
          if (read == 0)
            return;
          if (!connection.Frozen)
            await to.WriteAsync(buffer.AsMemory(0, read), Lifetime.Token);
        }
      }
      catch
      {
      }
    }

    public async ValueTask DisposeAsync()
    {
      Lifetime.Cancel();
      Listener.Stop();
      foreach (var connection in Connections)
        connection.Dispose();
      await AcceptLoop;
      Lifetime.Dispose();
    }

    sealed class ProxiedConnection(TcpClient downstream, TcpClient upstream) : IDisposable
    {
      public volatile bool Frozen;

      public void Dispose()
      {
        downstream.Dispose();
        upstream.Dispose();
      }
    }
  }

  // Host names.

  [Test]
  [Category("Regression")]
  public async Task ConnectorAndClientAcceptHostNames()
  {
    var connections = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          Observer = new DelegateSocketObserver { OnPeerConnected = _ => Interlocked.Increment(ref connections) }
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, cancellationToken) =>
          {
            // The first request closes the server's side, so the client must reconnect (and resolve
            // the name again) for the retry.
            if (request.Operation == "close-after")
              Task.Run(async () =>
              {
                await Task.Delay(50);
                await peer.DisposeAsync();
              });
            return ValueTask.FromResult(SocketResponse.Success());
          }
        });
    var port = listener.EndPoint.Port;

    await using var direct = await new SocketConnector(new SocketFabricOptions { NodeId = "client" })
        .ConnectAsync(new DnsEndPoint("localhost", port), new DelegateSocketPeerHandler());
    var viaConnector = await direct.RequestAsync("test", "op", [], TimeSpan.FromSeconds(5));

    await using var client = new SocketClient("localhost", port, new DelegateSocketPeerHandler(), new SocketFabricOptions { NodeId = "client" });
    var first = await client.RequestAsync("test", "close-after", 1);
    await Task.Delay(300);
    var second = await client.RequestAsync("test", "op", 1, new SocketClientRequestOptions
    {
      Idempotent = true,
      MaxAttempts = 3,
      RetryDelay = TimeSpan.FromMilliseconds(50)
    });

    Assert.That(viaConnector.Succeeded, Is.True);
    Assert.That(first.Succeeded, Is.True);
    Assert.That(second.Succeeded, Is.True, second.ErrorCode);
    Assert.That(Volatile.Read(ref connections), Is.GreaterThanOrEqualTo(3), "the client should have reconnected by name");
  }

  [Test]
  [Category("Regression")]
  public void UnresolvableHostNameFailsToConnect()
  {
    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client", HandshakeTimeout = TimeSpan.FromSeconds(2) });

    Assert.That(
        async () => await connector.ConnectAsync(new DnsEndPoint("pure-gate-socket-test.invalid", 9), new DelegateSocketPeerHandler()),
        Throws.InstanceOf<SocketException>().Or.InstanceOf<OperationCanceledException>());
  }

  // Event ordering.

  [Test]
  [Category("Regression")]
  public async Task SlowSequentialEventsDoNotDelayRequests()
  {
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var eventStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnEvent = async (_, _, _) =>
          {
            eventStarted.TrySetResult();
            await release.Task;
          },
          OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success())
        });
    await using var client = await ConnectAsync(listener);

    await client.SendEventAsync("test", "slow", [1]);
    await client.SendEventAsync("test", "slow", [2]);
    await eventStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
    var result = await client.RequestAsync("test", "ok", [], TimeSpan.FromSeconds(2));
    release.SetResult();

    Assert.That(result.Succeeded, Is.True, "a request waited behind a slow event");
  }

  [Test]
  [Category("Regression")]
  public async Task PerOperationOrderingKeepsEachOperationInOrderAndRunsOthersAlongside()
  {
    const int events = 300;
    var seen = new ConcurrentQueue<(string Operation, int Sequence)>();
    var slowBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", EventOrdering = SocketEventOrdering.PerOperation },
        new DelegateSocketPeerHandler
        {
          OnEvent = async (_, socketEvent, _) =>
          {
            var sequence = BinaryPrimitives.ReadInt32BigEndian(socketEvent.Payload.Span);
            if (socketEvent.Operation == "slow" && sequence == 0)
            {
              slowBlocked.TrySetResult();
              await release.Task;
            }

            seen.Enqueue((socketEvent.Operation, sequence));
          }
        });
    await using var client = await ConnectAsync(listener);

    for (var i = 0; i < events; i++)
    {
      var payload = new byte[4];
      BinaryPrimitives.WriteInt32BigEndian(payload, i);
      await client.SendEventAsync("test", i % 2 == 0 ? "slow" : "fast", payload);
    }

    // "slow" is stuck on its first event; "fast" must still finish every one of its events.
    await slowBlocked.Task.WaitAsync(TimeSpan.FromSeconds(5));
    await WaitUntilAsync(() => seen.Count(item => item.Operation == "fast") == events / 2, TimeSpan.FromSeconds(10));
    var fastDoneWhileSlowBlocked = seen.Count(item => item.Operation == "fast");
    release.SetResult();
    await WaitUntilAsync(() => seen.Count == events, TimeSpan.FromSeconds(10));

    Assert.That(fastDoneWhileSlowBlocked, Is.EqualTo(events / 2));
    Assert.That(seen.Where(item => item.Operation == "slow").Select(item => item.Sequence), Is.Ordered);
    Assert.That(seen.Where(item => item.Operation == "fast").Select(item => item.Sequence), Is.Ordered);
  }

  // Flow control.

  [Test]
  [Category("FlowControl")]
  public async Task ProducerNeverSendsPastAnUnreadConsumersWindow()
  {
    const int window = 128 * 1024;
    var observer = new StreamBytesObserver();
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", Observer = observer },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(8L * 1024 * 1024)))
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", StreamWindowBytes = window });

    await using var stream = await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(5));
    await Task.Delay(500);
    var sentWhileUnread = observer.StreamItemBytes;

    var buffer = new byte[64 * 1024];
    long position = 0;
    int read;
    while ((read = await stream.ReadAsync(buffer)) > 0)
    {
      for (var i = 0; i < read; i++)
      {
        if (buffer[i] != PatternStream.ByteAt(position + i))
          Assert.Fail($"corrupted byte at offset {position + i}");
      }

      position += read;
    }

    Assert.That(sentWhileUnread, Is.GreaterThan(0).And.LessThanOrEqualTo(window));
    Assert.That(position, Is.EqualTo(8L * 1024 * 1024));
  }

  [Test]
  [Category("FlowControl")]
  public async Task StreamWindowGrowsWhenItIsTheBottleneck()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(16L * 1024 * 1024)))
        });
    // Every write from the client (its credit grants) is delayed, as on a link with a long round trip.
    await using var client = await new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      StreamWindowBytes = 64 * 1024,
      MaxStreamWindowBytes = 8 * 1024 * 1024,
      StreamFilter = new DelayedWriteStreamFilter(TimeSpan.FromMilliseconds(10))
    }).ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await using var stream = await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(10));
    var total = await CountBytesAsync(stream);

    Assert.That(total, Is.EqualTo(16L * 1024 * 1024));
    Assert.That(((SocketPullStream)stream).WindowBytes, Is.GreaterThan(64 * 1024));
  }

  [Test]
  [Category("FlowControl")]
  public async Task StreamWindowDoesNotGrowForASlowReader()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(2L * 1024 * 1024)))
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", StreamWindowBytes = 256 * 1024 });

    await using var stream = await client.OpenStreamAsync("test", "pattern", [], TimeSpan.FromSeconds(10));
    await Task.Delay(200);
    var buffer = new byte[64 * 1024];
    long total = 0;
    int read;
    while ((read = await stream.ReadAsync(buffer)) > 0)
    {
      total += read;
      await Task.Delay(5);
    }

    Assert.That(total, Is.EqualTo(2L * 1024 * 1024));
    Assert.That(((SocketPullStream)stream).WindowBytes, Is.EqualTo(256 * 1024));
  }

  [Test]
  [Category("FlowControl")]
  public async Task ProducerThatOverrunsItsWindowClosesTheConnection()
  {
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    try
    {
      var server = Task.Run(async () =>
      {
        using var socket = await listener.AcceptTcpClientAsync();
        var stream = socket.GetStream();
        var reader = await RunRawNoAuthServerHandshakeAsync(stream);
        var writer = new SocketFrameWriter(stream, 1024 * 1024);
        var request = await reader.ReadAsync(CancellationToken.None);
        Assert.That(request.Kind, Is.EqualTo(SocketMessageKind.StreamRequest));
        Assert.That(request.Value, Is.EqualTo(64 * 1024), "the stream request should carry the consumer's window");
        await writer.WriteAsync(new SocketFrame
        {
          MessageId = request.MessageId,
          Kind = SocketMessageKind.StreamStart,
          Ok = true
        }, CancellationToken.None);
        await writer.WriteAsync(new SocketFrame
        {
          MessageId = request.MessageId,
          Kind = SocketMessageKind.StreamItem,
          Payload = new byte[request.Value + 1]
        }, CancellationToken.None);
        await Task.Delay(2000);
      });

      await using var client = await new SocketConnector(new SocketFabricOptions { NodeId = "client", StreamWindowBytes = 64 * 1024 })
          .ConnectAsync((IPEndPoint)listener.LocalEndpoint, new DelegateSocketPeerHandler());
      await using var stream = await client.OpenStreamAsync("test", "overrun", [], TimeSpan.FromSeconds(5));
      await client.Completion.WaitAsync(TimeSpan.FromSeconds(5));

      Assert.That(client.State, Is.EqualTo(SocketPeerState.Closed));
      await server;
    }
    finally
    {
      listener.Stop();
    }
  }

  [Test]
  [Category("FlowControl")]
  public async Task RequestThatTimesOutWaitingForCreditDoesNotLeakCredit()
  {
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1, MaxQueuedHandlers = 1 },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, request, _) =>
          {
            if (request.Operation == "hold")
            {
              started.TrySetResult();
              await release.Task;
            }

            return SocketResponse.Success();
          }
        });
    await using var client = await ConnectAsync(listener);

    // "hold" runs, "queued" takes the only credit, so "starved" has to wait for credit and times out.
    var hold = client.RequestAsync("test", "hold", [], TimeSpan.FromSeconds(10)).AsTask();
    await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
    var queued = client.RequestAsync("test", "queued", [], TimeSpan.FromSeconds(10)).AsTask();
    await Task.Delay(100);
    var starved = await client.RequestAsync("test", "starved", [], TimeSpan.FromMilliseconds(200));
    release.SetResult();
    await Task.WhenAll(hold, queued);

    // If the timed-out request had kept a credit, these would stall.
    var after = await Task.WhenAll(Enumerable.Range(0, 3)
        .Select(_ => client.RequestAsync("test", "after", [], TimeSpan.FromSeconds(5)).AsTask()));

    Assert.That(starved.ErrorCode, Is.EqualTo(SocketProtocolError.Timeout));
    Assert.That(after.Select(result => result.Succeeded), Has.All.True);
  }

  [Test]
  [Category("FlowControl")]
  public async Task SendersWaitingForCreditAreReleasedWhenTheConnectionCloses()
  {
    var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxConcurrentHandlers = 1, MaxQueuedHandlers = 1 },
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

    _ = client.RequestAsync("test", "running", [], TimeSpan.FromSeconds(30)).AsTask();
    await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
    _ = client.RequestAsync("test", "queued", [], TimeSpan.FromSeconds(30)).AsTask();
    await Task.Delay(100);
    var waitingRequest = client.RequestAsync("test", "waiting", [], TimeSpan.FromSeconds(30)).AsTask();
    var waitingEvent = client.SendEventAsync("test", "waiting", [1]).AsTask();
    await Task.Delay(100);
    await client.DisposeAsync();

    var result = await waitingRequest.WaitAsync(TimeSpan.FromSeconds(5));
    var eventError = await Assert.ThrowsAsync<SocketProtocolException>(async () => await waitingEvent.WaitAsync(TimeSpan.FromSeconds(5)));

    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.ConnectionClosed));
    Assert.That(eventError.ErrorCode, Is.EqualTo(SocketProtocolError.ConnectionClosed));
  }

  sealed class StreamBytesObserver : Observability.ISocketObserver
  {
    long Bytes;

    public long StreamItemBytes => Interlocked.Read(ref Bytes);

    public void FrameSent(SocketPeer peer, SocketMessageKind kind, int payloadBytes)
    {
      if (kind == SocketMessageKind.StreamItem)
        Interlocked.Add(ref Bytes, payloadBytes);
    }
  }

  sealed class DelayedWriteStreamFilter(TimeSpan delay) : ISocketStreamFilter
  {
    public ValueTask<Stream> WrapClientAsync(Stream stream, string localNodeId, CancellationToken cancellationToken)
    {
      return ValueTask.FromResult<Stream>(new DelayedWriteStream(stream, delay));
    }

    public ValueTask<Stream> WrapServerAsync(Stream stream, string localNodeId, CancellationToken cancellationToken)
    {
      return ValueTask.FromResult(stream);
    }
  }

  // Delays every write, so whatever this side sends (here: stream credit grants) arrives late.
  sealed class DelayedWriteStream(Stream inner, TimeSpan delay) : Stream
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

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
      return inner.ReadAsync(buffer, cancellationToken);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
      inner.Write(buffer, offset, count);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
      await Task.Delay(delay, cancellationToken);
      await inner.WriteAsync(buffer, cancellationToken);
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

  // Stress tests for races suspected from code reading. The races are narrow, so these mostly guard
  // future changes to the connection core.

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
  // Returns the reader so the caller keeps reading with it: a new reader would lose buffered bytes.
  static async ValueTask<SocketFrameReader> RunRawNoAuthServerHandshakeAsync(Stream stream)
  {
    var reader = new SocketFrameReader(stream, 1024 * 1024);
    var writer = new SocketFrameWriter(stream, 1024 * 1024);
    var hello = await reader.ReadAsync(CancellationToken.None);
    Assert.That(hello.Operation, Is.EqualTo("hello"));
    await writer.WriteAsync(new SocketFrame
    {
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
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "ready",
      Ok = true,
      Value = 4096
    }, CancellationToken.None);
    return reader;
  }

  static SocketFrame RawChunk(
      long messageId,
      int index,
      int count,
      int total,
      byte[] payload,
      SocketMessageKind originalKind = SocketMessageKind.Event)
  {
    return new SocketFrame
    {
      MessageId = messageId,
      Kind = SocketMessageKind.Chunk,
      OriginalKind = originalKind,
      ChunkIndex = index,
      ChunkCount = count,
      TotalPayloadLength = total,
      Payload = payload
    };
  }

  static SocketFrame RawEvent(long messageId, string operation, byte[] payload = null)
  {
    return new SocketFrame
    {
      MessageId = messageId,
      Kind = SocketMessageKind.Event,
      Service = "events",
      Operation = operation,
      Payload = payload ?? [1]
    };
  }

  // The encoded body of a frame (everything after the 4-byte length prefix), which is what chunks carry.
  static byte[] EncodeBody(SocketFrame frame)
  {
    using var output = new MemoryStream();
    new SocketFrameWriter(output, 16 * 1024 * 1024).WriteAsync(frame, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    return output.ToArray()[4..];
  }

  // Splits an encoded body into chunk frames the way the library does: equal sizes except the last.
  static List<SocketFrame> SplitIntoChunks(long messageId, SocketMessageKind originalKind, byte[] body, int chunkCount)
  {
    var chunkSize = (body.Length + chunkCount - 1) / chunkCount;
    var count = (body.Length + chunkSize - 1) / chunkSize;
    return Enumerable.Range(0, count)
        .Select(index => RawChunk(messageId, index, count, body.Length, body[(index * chunkSize)..Math.Min(body.Length, (index + 1) * chunkSize)], originalKind))
        .ToList();
  }

  sealed class HandlerFailedNamesObserver : Observability.ISocketObserver
  {
    public ConcurrentQueue<string> Operations { get; } = new();

    public void HandlerFailed(SocketPeer peer, string service, string operation, Exception exception)
    {
      Operations.Enqueue(operation);
    }
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
