using System.Buffers.Binary;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PureGate.Socket.Fabric;
using PureGate.Socket.Framing;
using PureGate.Socket.Handling;
using PureGate.Socket.Observability;
using PureGate.Socket.Protocol;
using PureGate.Socket.Security;
using PureGate.Socket.Serialization;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

public sealed class SocketFabricTests
{
  [Test]
  public async Task RequestAsync_ReturnsResponse()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "echo",
        new EchoRequest("hello"));

    Assert.That(response.Value, Is.EqualTo("hello"));
    Assert.That(client.RemoteNodeId, Is.EqualTo("server"));
  }

  [Test]
  public async Task RequestFastAsync_ReturnsResponse()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnFastRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse
          {
            Succeeded = request.OperationId == 7,
            Payload = request.Payload,
            ErrorCode = request.OperationId == 7 ? null : SocketProtocolError.UnsupportedOperation,
            ErrorMessage = request.OperationId == 7 ? null : "Unknown operation."
          })
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var payload = new byte[] { 1, 2, 3 };
    var response = await client.RequestFastAsync(7, payload, TimeSpan.FromSeconds(1));

    Assert.That(response.Succeeded, Is.True);
    Assert.That(response.Payload.ToArray(), Is.EqualTo(payload));
  }

  [Test]
  public async Task RequestFastBatchAsync_ReturnsPerItemResults()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnFastRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse
          {
            Succeeded = request.OperationId == 7,
            Payload = request.OperationId == 7 ? request.Payload : ReadOnlyMemory<byte>.Empty,
            ErrorCode = request.OperationId == 7 ? null : SocketProtocolError.UnsupportedOperation,
            ErrorMessage = request.OperationId == 7 ? null : "Unknown operation."
          })
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var results = await client.RequestFastBatchAsync(
        [
          new SocketFastBatchItem(1, 7, [1]),
          new SocketFastBatchItem(2, 8, [2]),
          new SocketFastBatchItem(3, 7, [3])
        ],
        TimeSpan.FromSeconds(1));

    Assert.That(results.Count, Is.EqualTo(3));
    Assert.That(results[0].Succeeded, Is.True);
    Assert.That(results[0].Payload, Is.EqualTo(new byte[] { 1 }));
    Assert.That(results[1].Succeeded, Is.False);
    Assert.That(results[1].ErrorCode, Is.EqualTo(SocketProtocolError.UnsupportedOperation));
    Assert.That(results[2].Succeeded, Is.True);
    Assert.That(results[2].Payload, Is.EqualTo(new byte[] { 3 }));
  }

  [Test]
  public async Task RequestFastAsync_ReturnsUnauthorizedWithoutCallingHandler()
  {
    var handlerCalls = 0;
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Authorizer = new DelegateSocketAuthorizer
      {
        OnFastRequest = (_, request, _) => ValueTask.FromResult(
            request.OperationId == 7
                ? SocketAuthorizationResult.Denied("no fast")
                : SocketAuthorizationResult.Allowed)
      }
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnFastRequest = (_, request, _) =>
          {
            Interlocked.Increment(ref handlerCalls);
            return ValueTask.FromResult(new SocketResponse
            {
              Succeeded = true,
              Payload = request.Payload
            });
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var response = await client.RequestFastAsync(7, [1, 2, 3], TimeSpan.FromSeconds(1));

    Assert.That(response.Succeeded, Is.False);
    Assert.That(response.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
    Assert.That(handlerCalls, Is.EqualTo(0));
  }

  [Test]
  public async Task RequestFastBatchAsync_AuthorizesEachItemWithoutClosingPeer()
  {
    var handlerCalls = 0;
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Authorizer = new DelegateSocketAuthorizer
      {
        OnFastRequest = (_, request, _) => ValueTask.FromResult(
            request.OperationId == 8
                ? SocketAuthorizationResult.Denied("denied item")
                : SocketAuthorizationResult.Allowed)
      }
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnFastRequest = (_, request, _) =>
          {
            Interlocked.Increment(ref handlerCalls);
            return ValueTask.FromResult(new SocketResponse
            {
              Succeeded = true,
              Payload = request.Payload
            });
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var results = await client.RequestFastBatchAsync(
        [
          new SocketFastBatchItem(1, 7, [1]),
          new SocketFastBatchItem(2, 8, [2]),
          new SocketFastBatchItem(3, 7, [3])
        ],
        TimeSpan.FromSeconds(1));
    var afterDeniedItem = await client.RequestFastAsync(7, [4], TimeSpan.FromSeconds(1));

    Assert.That(results.Count, Is.EqualTo(3));
    Assert.That(results[0].Succeeded, Is.True);
    Assert.That(results[1].Succeeded, Is.False);
    Assert.That(results[1].ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
    Assert.That(results[2].Succeeded, Is.True);
    Assert.That(afterDeniedItem.Succeeded, Is.True);
    Assert.That(handlerCalls, Is.EqualTo(3));
  }

  [Test]
  public async Task RequestFastBatchAsync_RejectsEmptyBatch()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await Assert.ThrowsAsync<ArgumentException>(async () =>
        await client.RequestFastBatchAsync([], TimeSpan.FromSeconds(1)));
  }

  [Test]
  public async Task RequestFastBatchAsync_RejectsNestedFastBatchItem()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnFastRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse
          {
            Succeeded = true,
            Payload = request.Payload
          })
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var results = await client.RequestFastBatchAsync(
        [
          new SocketFastBatchItem(1, int.MinValue, [])
        ],
        TimeSpan.FromSeconds(1));

    Assert.That(results.Count, Is.EqualTo(1));
    Assert.That(results[0].Succeeded, Is.False);
    Assert.That(results[0].ErrorCode, Is.EqualTo(SocketProtocolError.ProtocolError));
  }

  [Test]
  public async Task StartAsync_DoesNotUseCallerCancellationAsListenerLifetime()
  {
    using var startCancellation = new CancellationTokenSource();
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        },
        startCancellation.Token);

    startCancellation.Cancel();

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "echo",
        new EchoRequest("still-listening"));

    Assert.That(response.Value, Is.EqualTo("still-listening"));
  }

  [Test]
  public async Task Peer_CanSendRequestBackToConnector()
  {
    var serverPeerSource = new TaskCompletionSource<SocketPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnConnect = (peer, _) =>
          {
            serverPeerSource.TrySetResult(peer);
            return ValueTask.CompletedTask;
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(
        listener.EndPoint,
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, new EchoRequest("from-client")))
        });

    var serverPeer = await serverPeerSource.Task.WaitAsync(TimeSpan.FromSeconds(5));
    var response = await serverPeer.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "reverse",
        new EchoRequest("from-server"));

    Assert.That(response.Value, Is.EqualTo("from-client"));
  }

  [Test]
  public async Task RequestHandler_CanSendReverseRequestWithoutBlockingReadLoop()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = async (peer, request, _) =>
          {
            var reverse = await peer.RequestAsync<EchoRequest, EchoRequest>(
                "test",
                "inside-handler",
                new EchoRequest("server-callback"));
            return SocketResponse.Success(peer.Codec, reverse);
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(
        listener.EndPoint,
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, new EchoRequest("client-response")))
        });

    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "outer",
        new EchoRequest("client-request"));

    Assert.That(response.Value, Is.EqualTo("client-response"));
  }

  [Test]
  public async Task RequestBatchAsync_SendsMultipleRequestsInOneEnvelope()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) =>
          {
            if (request.Operation == "fail")
              return ValueTask.FromResult(SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "expected"));

            return ValueTask.FromResult(SocketResponse.Success(
                peer.Codec,
                request.ReadPayload<EchoRequest>(peer.Codec)));
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var results = await client.RequestBatchAsync(
        [
          new SocketBatchItem(1, "test", "echo", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("one"))),
          new SocketBatchItem(2, "test", "fail", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("two"))),
          new SocketBatchItem(3, "test", "echo", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("three")))
        ],
        TimeSpan.FromSeconds(5));

    Assert.That(results, Has.Count.EqualTo(3));
    Assert.That(results[0].Succeeded, Is.True);
    Assert.That(JsonSocketMessageCodec.Shared.Deserialize<EchoRequest>(results[0].Payload).Value, Is.EqualTo("one"));
    Assert.That(results[1].Succeeded, Is.False);
    Assert.That(results[1].ErrorCode, Is.EqualTo(SocketProtocolError.UnsupportedOperation));
    Assert.That(results[2].Succeeded, Is.True);
    Assert.That(JsonSocketMessageCodec.Shared.Deserialize<EchoRequest>(results[2].Payload).Value, Is.EqualTo("three"));
  }

  [Test]
  public async Task SendEventAsync_DeliversEvent()
  {
    var eventSource = new TaskCompletionSource<EchoRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnEvent = (peer, socketEvent, _) =>
          {
            eventSource.TrySetResult(socketEvent.ReadPayload<EchoRequest>(peer.Codec));
            return ValueTask.CompletedTask;
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await client.SendEventAsync("events", "notice", new EchoRequest("wake-up"));

    var received = await eventSource.Task.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.That(received.Value, Is.EqualTo("wake-up"));
  }

  [Test]
  public async Task SendEventBatchAsync_SendsMultipleEventsInOneEnvelope()
  {
    var received = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
    var values = new List<string>();
    var sync = new Lock();
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, socketEvent, _) =>
          {
            lock (sync)
            {
              values.Add(socketEvent.ReadPayload<EchoRequest>(JsonSocketMessageCodec.Shared).Value);
              if (values.Count == 3)
                received.TrySetResult([.. values]);
            }

            return ValueTask.CompletedTask;
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await client.SendEventBatchAsync(
        [
          new SocketBatchItem(1, "events", "notice", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("one"))),
          new SocketBatchItem(2, "events", "notice", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("two"))),
          new SocketBatchItem(3, "events", "notice", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("three")))
        ]);

    var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(result, Is.EquivalentTo(new[] { "one", "two", "three" }));
  }

  [Test]
  public async Task SendEventBatchWithResultsAsync_ReturnsPerItemResultsWithoutClosingPeer()
  {
    var delivered = new List<string>();
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Authorizer = new DelegateSocketAuthorizer
      {
        OnEvent = (_, socketEvent, _) => ValueTask.FromResult(
            socketEvent.Operation == "denied"
                ? SocketAuthorizationResult.Denied("nope")
                : SocketAuthorizationResult.Allowed)
      }
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, socketEvent, _) =>
          {
            delivered.Add(socketEvent.ReadPayload<EchoRequest>(JsonSocketMessageCodec.Shared).Value);
            return ValueTask.CompletedTask;
          },
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var results = await client.SendEventBatchWithResultsAsync(
        [
          new SocketBatchItem(1, "events", "allowed", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("one"))),
          new SocketBatchItem(2, "events", "denied", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("two"))),
          new SocketBatchItem(3, "events", "allowed", JsonSocketMessageCodec.Shared.Serialize(new EchoRequest("three")))
        ],
        TimeSpan.FromSeconds(5));
    var stillConnected = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "echo",
        new EchoRequest("alive"));

    Assert.That(results, Has.Count.EqualTo(3));
    Assert.That(results[0].Succeeded, Is.True);
    Assert.That(results[1].Succeeded, Is.False);
    Assert.That(results[1].ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
    Assert.That(results[2].Succeeded, Is.True);
    Assert.That(delivered, Is.EquivalentTo(new[] { "one", "three" }));
    Assert.That(stillConnected.Value, Is.EqualTo("alive"));
  }

  [Test]
  public void SocketFabricOptions_NormalizeKeepsIdleTimeoutAboveHeartbeatInterval()
  {
    var options = new SocketFabricOptions
    {
      HeartbeatInterval = TimeSpan.FromMilliseconds(100),
      IdleTimeout = TimeSpan.FromMilliseconds(1)
    }.Normalize();

    Assert.That(options.HeartbeatInterval, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
    Assert.That(options.IdleTimeout, Is.EqualTo(TimeSpan.FromMilliseconds(300)));

    var defaultedIdleTimeout = new SocketFabricOptions
    {
      HeartbeatInterval = TimeSpan.FromSeconds(20),
      IdleTimeout = TimeSpan.Zero
    }.Normalize();

    Assert.That(defaultedIdleTimeout.IdleTimeout, Is.EqualTo(TimeSpan.FromSeconds(60)));
  }

  [Test]
  public void SocketClientOptions_NormalizeKeepsReconnectPolicyBounded()
  {
    var options = new SocketClientOptions
    {
      KeepConnected = true,
      ReconnectInitialDelay = TimeSpan.Zero,
      ReconnectMaxDelay = TimeSpan.FromMilliseconds(1),
      ReconnectJitterRatio = 2
    }.Normalize();

    Assert.That(options.KeepConnected, Is.True);
    Assert.That(options.ReconnectInitialDelay, Is.GreaterThan(TimeSpan.Zero));
    Assert.That(options.ReconnectMaxDelay, Is.GreaterThanOrEqualTo(options.ReconnectInitialDelay));
    Assert.That(options.ReconnectJitterRatio, Is.EqualTo(1));
  }

  [Test]
  public async Task Peer_RepliesToPingWithPong()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    using var tcp = new TcpClient(AddressFamily.InterNetwork);
    await tcp.ConnectAsync(listener.EndPoint);
    var stream = tcp.GetStream();
    await RunRawNoAuthHandshakeAsync(stream);

    var writer = new SocketFrameWriter(stream, 1024);
    var reader = new SocketFrameReader(stream, 1024);
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      MessageId = 42,
      Kind = SocketMessageKind.Ping
    }, CancellationToken.None);

    var pong = await reader.ReadAsync(CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(pong, Is.Not.Null);
    Assert.That(pong.Kind, Is.EqualTo(SocketMessageKind.Pong));
    Assert.That(pong.MessageId, Is.EqualTo(42));
    Assert.That(pong.Ok, Is.True);
  }

  [Test]
  public async Task Heartbeat_KeepsNormalPeerAlivePastIdleTimeout()
  {
    var serverOptions = new SocketFabricOptions
    {
      NodeId = "server",
      HeartbeatInterval = TimeSpan.FromMilliseconds(30),
      IdleTimeout = TimeSpan.FromMilliseconds(90)
    };
    await using var listener = new SocketListener(serverOptions);
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      HeartbeatInterval = TimeSpan.FromMilliseconds(30),
      IdleTimeout = TimeSpan.FromMilliseconds(90)
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await Task.Delay(TimeSpan.FromMilliseconds(220));
    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "echo",
        new EchoRequest("alive"),
        CancellationToken.None);

    Assert.That(response.Value, Is.EqualTo("alive"));
  }

  [Test]
  public async Task Heartbeat_ClosesSilentPeerAfterIdleTimeout()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      HeartbeatInterval = TimeSpan.FromMilliseconds(30),
      IdleTimeout = TimeSpan.FromMilliseconds(90)
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    using var tcp = new TcpClient(AddressFamily.InterNetwork);
    await tcp.ConnectAsync(listener.EndPoint);
    var stream = tcp.GetStream();
    await RunRawNoAuthHandshakeAsync(stream);

    var reader = new SocketFrameReader(stream, 1024);
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    while (true)
    {
      var frame = await reader.ReadAsync(timeout.Token);
      if (frame is null)
        break;

      Assert.That(frame.Kind, Is.EqualTo(SocketMessageKind.Ping));
    }
  }

  [Test]
  public async Task OpenStreamAsync_PullsRemoteStreamWithoutBufferingWholePayload()
  {
    var payload = Enumerable.Range(0, 200_000).Select(index => (byte)(index % 251)).ToArray();
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 2048,
      StreamChunkBytes = 333
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnStream = (_, request, _) =>
          {
            Assert.That(request.ReadPayload<EchoRequest>(JsonSocketMessageCodec.Shared).Value, Is.EqualTo("artifact"));
            return ValueTask.FromResult(SocketStreamResponse.Success(new MemoryStream(payload)));
          }
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions
        {
          NodeId = "client",
          MaxFrameSizeBytes = 2048,
          StreamChunkBytes = 333
        });

    await using var stream = await client.OpenStreamAsync(
        "blob",
        "pull",
        new EchoRequest("artifact"));
    using var output = new MemoryStream();
    await stream.CopyToAsync(output);

    Assert.That(output.ToArray(), Is.EqualTo(payload));
  }

  [Test]
  public async Task OpenStreamAsync_FailsBeforeReturningStreamWhenUnauthorized()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Authorizer = new DelegateSocketAuthorizer
      {
        OnStream = (_, _, _) => ValueTask.FromResult(SocketAuthorizationResult.Denied("no stream"))
      }
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(Stream.Null))
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions { NodeId = "client" });

    var exception = await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await client.OpenStreamAsync("blob", "pull", new EchoRequest("denied")));

    Assert.That(exception.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
  }

  [Test]
  public async Task OpenStreamAsync_SurfaceRemoteFailureDuringRead()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult<SocketStreamResponse>(
              SocketStreamResponse.Success(new FailingReadStream([1, 2, 3])))
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions { NodeId = "client" });

    await using var stream = await client.OpenStreamAsync("blob", "pull", new EchoRequest("fail"));
    var buffer = new byte[8];
    var read = await stream.ReadAtLeastAsync(buffer, 3, throwOnEndOfStream: false);

    Assert.That(read, Is.EqualTo(3));
    var exception = await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await stream.ReadAtLeastAsync(buffer, 1, throwOnEndOfStream: false));
    Assert.That(exception.ErrorCode, Is.EqualTo(SocketProtocolError.HandlerFailed));
  }

  [Test]
  public async Task OpenStreamAsync_ReportsStreamFailureToObserver()
  {
    var completed = new TaskCompletionSource<(bool Succeeded, string ErrorCode)>(
        TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Observer = new DelegateSocketObserver
      {
        OnRequestCompleted = (_, service, operation, succeeded, errorCode) =>
        {
          if (service == "blob" && operation == "pull")
            completed.TrySetResult((succeeded, errorCode));
        }
      }
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnStream = (_, _, _) => ValueTask.FromResult(
              SocketStreamResponse.Failed(SocketProtocolError.UnsupportedOperation, "nope"))
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions { NodeId = "client" });

    await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await client.OpenStreamAsync("blob", "pull", new EchoRequest("fail")));
    var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(result.Succeeded, Is.False);
    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.UnsupportedOperation));
  }

  [Test]
  public async Task SendEventAsync_CarriesConcurrentChunkedEventsWithoutCollision()
  {
    var received = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
    var values = new List<string>();
    var sync = new Lock();
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 1024,
      MaxChunkPayloadBytes = 128
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnEvent = (_, socketEvent, _) =>
          {
            lock (sync)
            {
              values.Add(socketEvent.ReadPayload<EchoRequest>(JsonSocketMessageCodec.Shared).Value);
              if (values.Count == 2)
                received.TrySetResult([.. values]);
            }

            return ValueTask.CompletedTask;
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      MaxFrameSizeBytes = 1024,
      MaxChunkPayloadBytes = 128
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var first = new EchoRequest(new string('a', 16 * 1024));
    var second = new EchoRequest(new string('b', 16 * 1024));
    await Task.WhenAll(
        client.SendEventAsync("events", "large", first).AsTask(),
        client.SendEventAsync("events", "large", second).AsTask());

    var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(result, Has.Count.EqualTo(2));
    Assert.That(result, Does.Contain(first.Value));
    Assert.That(result, Does.Contain(second.Value));
  }

  [Test]
  public async Task HmacAuthenticator_RejectsWrongSecret()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Authenticator = new HmacSocketAuthenticator("server-secret")
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      Authenticator = new HmacSocketAuthenticator("client-secret")
    });

    var ex = await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler()));
    Assert.That(ex.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
  }

  [Test]
  public async Task HmacAuthenticator_AllowsMatchingSecret()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Authenticator = new HmacSocketAuthenticator("cluster-secret")
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      Authenticator = new HmacSocketAuthenticator("cluster-secret")
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "echo",
        new EchoRequest("authenticated"));

    Assert.That(response.Value, Is.EqualTo("authenticated"));
  }

  [Test]
  public async Task Authorizer_DeniesUnauthorizedRequestWithoutCallingHandler()
  {
    var handlerCalled = false;
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Authorizer = new DelegateSocketAuthorizer
      {
        OnRequest = (_, request, _) => ValueTask.FromResult(
            request.Operation == "allowed"
                ? SocketAuthorizationResult.Allowed
                : SocketAuthorizationResult.Denied("nope"))
      }
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, _, _) =>
          {
            handlerCalled = true;
            return ValueTask.FromResult(SocketResponse.Success());
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var result = await client.RequestAsync("test", "denied", [], TimeSpan.FromSeconds(1));

    Assert.That(result.Succeeded, Is.False);
    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
    Assert.That(handlerCalled, Is.False);
  }

  [Test]
  public async Task SocketPeerRegistry_TracksPeersAndCanSendReverseRequest()
  {
    var registry = new SocketPeerRegistry();
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Observer = registry
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(
        listener.EndPoint,
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var serverPeer = await WaitForPeerAsync(registry, "client");
    var response = await serverPeer.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "registry",
        new EchoRequest("from-registry"));

    Assert.That(response.Value, Is.EqualTo("from-registry"));
  }

  [Test]
  public async Task SocketPeerRegistry_TracksMultipleSessionsForSameNode()
  {
    var registry = new SocketPeerRegistry();
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Observer = registry
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var first = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await using var second = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await WaitForPeerCountAsync(registry, "client", 2);

    Assert.That(registry.Count, Is.EqualTo(2));
    Assert.That(registry.Snapshot("client").Select(peer => peer.ConnectionId), Is.Unique);
  }

  [Test]
  public async Task SocketPeerRegistry_RoundRobinSelectsAcrossNodeSessions()
  {
    var registry = new SocketPeerRegistry();
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Observer = registry
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var first = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await using var second = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await WaitForPeerCountAsync(registry, "client", 2);

    var selected = Enumerable
        .Range(0, 8)
        .Select(_ => registry.Get("client", SocketPeerSelectionPolicy.RoundRobin).ConnectionId)
        .Distinct()
        .ToArray();

    Assert.That(selected, Has.Length.EqualTo(2));
  }

  [Test]
  public async Task SocketClient_ReconnectsOnNextUseAfterClose()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions { NodeId = "client" });

    var first = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "first",
        new EchoRequest("first"));
    await client.CloseAsync();
    var second = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "second",
        new EchoRequest("second"));

    Assert.That(first.Value, Is.EqualTo("first"));
    Assert.That(second.Value, Is.EqualTo("second"));
  }

  [Test]
  public async Task SocketClient_KeepConnectedConnectsBeforeFirstRequest()
  {
    var connected = new TaskCompletionSource<SocketPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions
        {
          NodeId = "client",
          Observer = new DelegateSocketObserver
          {
            OnPeerConnected = peer => connected.TrySetResult(peer)
          }
        },
        new SocketClientOptions
        {
          KeepConnected = true,
          ReconnectInitialDelay = TimeSpan.FromMilliseconds(10),
          ReconnectMaxDelay = TimeSpan.FromMilliseconds(20),
          ReconnectJitterRatio = 0
        });

    var peer = await connected.Task.WaitAsync(TimeSpan.FromSeconds(5));
    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "echo",
        new EchoRequest("already-connected"));

    Assert.That(peer.RemoteNodeId, Is.EqualTo("server"));
    Assert.That(response.Value, Is.EqualTo("already-connected"));
  }

  [Test]
  public async Task SocketClient_KeepConnectedReconnectsAfterClose()
  {
    var connectedCount = 0;
    var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions
        {
          NodeId = "client",
          Observer = new DelegateSocketObserver
          {
            OnPeerConnected = _ =>
            {
              if (Interlocked.Increment(ref connectedCount) == 2)
                reconnected.TrySetResult();
            }
          }
        },
        new SocketClientOptions
        {
          KeepConnected = true,
          ReconnectInitialDelay = TimeSpan.FromMilliseconds(10),
          ReconnectMaxDelay = TimeSpan.FromMilliseconds(20),
          ReconnectJitterRatio = 0
        });

    await client.GetPeerAsync();
    await client.CloseAsync();
    await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "echo",
        new EchoRequest("reconnected"));

    Assert.That(response.Value, Is.EqualTo("reconnected"));
    Assert.That(connectedCount, Is.GreaterThanOrEqualTo(2));
  }

  [Test]
  public async Task SocketClient_KeepConnectedBacksOffAfterConnectFailure()
  {
    var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
    var scheduled = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
    var endpoint = ReserveUnusedLoopbackEndPoint();
    await using var client = new SocketClient(
        endpoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions
        {
          NodeId = "client",
          Observer = new DelegateSocketObserver
          {
            OnClientConnectFailed = (_, exception) => failed.TrySetResult(exception),
            OnClientReconnectScheduled = (_, delay) => scheduled.TrySetResult(delay)
          }
        },
        new SocketClientOptions
        {
          KeepConnected = true,
          ReconnectInitialDelay = TimeSpan.FromMilliseconds(10),
          ReconnectMaxDelay = TimeSpan.FromMilliseconds(20),
          ReconnectJitterRatio = 0
        });

    var exception = await failed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    var delay = await scheduled.Task.WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(exception, Is.Not.Null);
    Assert.That(delay, Is.EqualTo(TimeSpan.FromMilliseconds(10)));
  }

  [Test]
  public async Task SocketClient_ReplaysIdempotentRetryWithoutCallingHandlerTwice()
  {
    var noRetryAttempts = 0;
    var retryAttempts = 0;
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = async (peer, request, _) =>
          {
            if (request.Operation == "no-retry")
            {
              noRetryAttempts++;
              await peer.DisposeAsync();
              return SocketResponse.Success(peer.Codec, new EchoRequest("never-seen"));
            }

            retryAttempts++;
            if (retryAttempts == 1)
            {
              await peer.DisposeAsync();
              return SocketResponse.Success(peer.Codec, new EchoRequest("applied-once"));
            }

            return SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec));
          }
        });

    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions { NodeId = "client" });

    var failed = await client.RequestAsync(
        "test",
        "no-retry",
        new EchoRequest("without-retry"),
        SocketClientRequestOptions.Default);
    var response = await client.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "retry",
        new EchoRequest("with-retry"),
        new SocketClientRequestOptions
        {
          Idempotent = true,
          MaxAttempts = 2,
          RetryDelay = TimeSpan.Zero
        });

    Assert.That(failed.Succeeded, Is.False);
    Assert.That(failed.ErrorCode, Is.AnyOf(SocketProtocolError.ConnectionClosed, SocketProtocolError.Timeout));
    Assert.That(response.Value, Is.EqualTo("applied-once"));
    Assert.That(noRetryAttempts, Is.EqualTo(1));
    Assert.That(retryAttempts, Is.EqualTo(1));
  }

  [Test]
  public void CompositeSocketObserver_FansOutAndIsolatesObserverFailures()
  {
    var peerAcceptFailed = false;
    var listenerAcceptFailed = false;
    var clientLoopFailed = false;
    var observer = new CompositeSocketObserver(
        new ThrowingSocketObserver(),
        new DelegateSocketObserver
        {
          OnPeerAcceptFailed = (_, _) => peerAcceptFailed = true,
          OnListenerAcceptFailed = (_, _) => listenerAcceptFailed = true,
          OnClientKeepConnectedLoopFailed = (_, _) => clientLoopFailed = true
        });

    observer.PeerAcceptFailed(new IPEndPoint(IPAddress.Loopback, 1234), new InvalidOperationException("test"));
    observer.ListenerAcceptFailed(new IPEndPoint(IPAddress.Loopback, 1234), new InvalidOperationException("test"));
    observer.ClientKeepConnectedLoopFailed(new IPEndPoint(IPAddress.Loopback, 1234), new InvalidOperationException("test"));

    Assert.That(peerAcceptFailed, Is.True);
    Assert.That(listenerAcceptFailed, Is.True);
    Assert.That(clientLoopFailed, Is.True);
  }

  [Test]
  public async Task RequestAsync_TimesOutWhenPeerDoesNotRespond()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, _, cancellationToken) =>
          {
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            return SocketResponse.Success();
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      RequestTimeout = TimeSpan.FromMilliseconds(100)
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var result = await client.RequestAsync(
        "test",
        "slow",
        [],
        TimeSpan.FromMilliseconds(100));

    Assert.That(result.Succeeded, Is.False);
    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.Timeout));
  }

  [Test]
  public async Task RequestAsync_CancelsRemoteHandlerWhenCallerCancels()
  {
    var cancelledSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = async (_, _, cancellationToken) =>
          {
            try
            {
              await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            }
            catch (OperationCanceledException)
            {
              cancelledSource.TrySetResult();
              throw;
            }

            return SocketResponse.Success();
          }
        });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

    var result = await client.RequestAsync(
        "test",
        "cancel",
        [],
        TimeSpan.FromSeconds(10),
        cancellationToken: cancellation.Token);

    Assert.That(result.Succeeded, Is.False);
    Assert.That(result.ErrorCode, Is.EqualTo(SocketProtocolError.Cancelled));
    await cancelledSource.Task.WaitAsync(TimeSpan.FromSeconds(5));
  }

  [Test]
  public async Task RawSocketMessageCodec_SendsBytesWithoutJsonPayloadSerialization()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      Codec = RawSocketMessageCodec.Shared
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<byte[]>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      Codec = RawSocketMessageCodec.Shared
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var response = await client.RequestAsync<byte[], byte[]>(
        "test",
        "bytes",
        [1, 2, 3, 4]);

    Assert.That(response, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
  }

  [Test]
  public async Task TlsSocketStreamFilter_CarriesAuthenticatedSocketTraffic()
  {
    using var certificate = CreateTestCertificate();
    var tls = TlsSocketStreamFilter.Create(
        certificate,
        "puregate.socket",
        serverCertificateValidation: (_, _, _, _) => true);
    var acceptFailure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      StreamFilter = tls,
      Observer = new DelegateSocketObserver
      {
        OnPeerAcceptFailed = (_, exception) =>
        {
          acceptFailure.TrySetResult(exception);
        }
      }
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      StreamFilter = tls
    });
    SocketPeer client = null;
    try
    {
      client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    }
    catch (Exception ex)
    {
      var failure = await Task.WhenAny(acceptFailure.Task, Task.Delay(TimeSpan.FromSeconds(1))) == acceptFailure.Task
          ? acceptFailure.Task.Result.ToString()
          : "Server failure was not reported.";
      Assert.Fail($"Client TLS failure: {ex}\nServer TLS failure: {failure}");
    }

    await using var disposableClient = client;

    var response = await disposableClient.RequestAsync<EchoRequest, EchoRequest>(
        "test",
        "tls",
        new EchoRequest("encrypted"));

    Assert.That(response.Value, Is.EqualTo("encrypted"));
  }

  [Test]
  public async Task BrotliPayloadCompression_CanCarryPayloadLargerThanCompressedFrameLimit()
  {
    var repeated = new byte[64 * 1024];
    Array.Fill(repeated, (byte)'x');
    var serverOptions = new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 1024,
      Codec = RawSocketMessageCodec.Shared,
      PayloadCompression = SocketPayloadCompression.Brotli,
      CompressionMinPayloadBytes = 0
    };
    var clientOptions = new SocketFabricOptions
    {
      NodeId = "client",
      MaxFrameSizeBytes = 1024,
      Codec = RawSocketMessageCodec.Shared,
      PayloadCompression = SocketPayloadCompression.Brotli,
      CompressionMinPayloadBytes = 0
    };

    await using var listener = new SocketListener(serverOptions);
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<byte[]>(peer.Codec)))
        });

    var connector = new SocketConnector(clientOptions);
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var response = await client.RequestAsync<byte[], byte[]>(
        "test",
        "compressed",
        repeated);

    Assert.That(response, Is.EqualTo(repeated));
  }

  [Test]
  public async Task RequestAsync_HandlesManyConcurrentRequests()
  {
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      MaxInFlightRequests = 128,
      MaxConcurrentHandlers = 32
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var tasks = Enumerable
        .Range(0, 128)
        .Select(index => client.RequestAsync<EchoRequest, EchoRequest>(
            "test",
            "parallel",
            new EchoRequest(index.ToString())).AsTask())
        .ToArray();

    var responses = await Task.WhenAll(tasks);

    Assert.That(responses.Select(response => response.Value), Is.EquivalentTo(Enumerable.Range(0, 128).Select(index => index.ToString())));
  }

  [Test]
  public async Task Chunking_CarriesLargeNonCompressiblePayloadAcrossSmallFrames()
  {
    var payload = RandomNumberGenerator.GetBytes(16 * 1024);
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 1024,
      MaxChunkPayloadBytes = 128,
      Codec = RawSocketMessageCodec.Shared
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<byte[]>(peer.Codec)))
        });

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      MaxFrameSizeBytes = 1024,
      MaxChunkPayloadBytes = 128,
      Codec = RawSocketMessageCodec.Shared
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var response = await client.RequestAsync<byte[], byte[]>(
        "test",
        "chunked",
        payload);

    Assert.That(response, Is.EqualTo(payload));
  }

  [Test]
  public async Task Chunking_RejectsTooManyIncompleteChunkedMessages()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 1024,
      MaxChunkPayloadBytes = 128,
      MaxChunkedMessages = 1,
      MaxConcurrentHandlers = 1
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    using var tcp = new TcpClient(AddressFamily.InterNetwork);
    await tcp.ConnectAsync(listener.EndPoint);
    await RunRawNoAuthHandshakeAsync(tcp.GetStream());
    var writer = new SocketFrameWriter(tcp.GetStream(), 1024);
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      MessageId = 1,
      Kind = SocketMessageKind.Chunk,
      OriginalKind = SocketMessageKind.Event,
      Service = "events",
      Operation = "one",
      ChunkIndex = 0,
      ChunkCount = 2,
      TotalPayloadLength = 10,
      Payload = [1, 2, 3, 4, 5]
    }, CancellationToken.None);
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      MessageId = 2,
      Kind = SocketMessageKind.Chunk,
      OriginalKind = SocketMessageKind.Event,
      Service = "events",
      Operation = "two",
      ChunkIndex = 0,
      ChunkCount = 2,
      TotalPayloadLength = 10,
      Payload = [1, 2, 3, 4, 5]
    }, CancellationToken.None);

    var buffer = new byte[1];
    var read = await tcp.GetStream().ReadAsync(buffer.AsMemory()).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

    Assert.That(read, Is.EqualTo(0));
  }

  [Test]
  public async Task SendEventAsync_RejectsFrameThatCannotBeChunked()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 1024
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler());

    var connector = new SocketConnector(new SocketFabricOptions
    {
      NodeId = "client",
      MaxFrameSizeBytes = 1024
    });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await client.SendEventAsync(new string('s', 2048), "large", []));
  }

  [Test]
  public async Task SocketFrameReader_RejectsNegativeStringLength()
  {
    using var stream = new MemoryStream(BuildFrameWithNegativeServiceLength());
    var reader = new SocketFrameReader(stream, 1024);

    await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await reader.ReadAsync(CancellationToken.None));
  }

  [Test]
  public async Task SocketFrameReader_RejectsFastFrameTrailingBytes()
  {
    using var stream = new MemoryStream(BuildFastRequestFrameWithTrailingByte());
    var reader = new SocketFrameReader(stream, 1024);

    await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await reader.ReadAsync(CancellationToken.None));
  }

  [Test]
  public async Task SocketFrameReader_RejectsFastFrameNegativePayloadLength()
  {
    using var stream = new MemoryStream(BuildFastRequestFrameWithNegativePayloadLength());
    var reader = new SocketFrameReader(stream, 1024);

    await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await reader.ReadAsync(CancellationToken.None));
  }

  [Test]
  public void SocketIdempotencyCache_PrunesOldestEntriesWhenOverLimit()
  {
    var cache = new SocketIdempotencyCache();
    var first = new SocketIdempotencyKey("node", "service", "operation", "1");
    var second = new SocketIdempotencyKey("node", "service", "operation", "2");
    var third = new SocketIdempotencyKey("node", "service", "operation", "3");

    cache.GetOrAdd(first, CompletedIdempotencyEntry());
    cache.GetOrAdd(second, CompletedIdempotencyEntry());
    cache.GetOrAdd(third, CompletedIdempotencyEntry());

    cache.Prune(TimeSpan.FromHours(1), maxEntries: 2);

    Assert.That(cache.Count, Is.EqualTo(2));
    Assert.That(cache.Contains(first), Is.False);
    Assert.That(cache.Contains(second), Is.True);
    Assert.That(cache.Contains(third), Is.True);
  }

  [Test]
  public async Task SocketFabric_HandlesConcurrentRequestPressure()
  {
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxConcurrentHandlers = 32,
      MaxInFlightRequests = 512
    });
    await listener.StartAsync(
        new IPEndPoint(IPAddress.Loopback, 0),
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, request.ReadPayload<EchoRequest>(peer.Codec)))
        });

    var clients = new List<SocketClient>();
    try
    {
      for (var i = 0; i < 4; i++)
      {
        clients.Add(new SocketClient(
            listener.EndPoint,
            new DelegateSocketPeerHandler(),
            new SocketFabricOptions
            {
              NodeId = $"client-{i}",
              MaxInFlightRequests = 512
            }));
      }

      var tasks = Enumerable.Range(0, 200)
          .Select(async index =>
          {
            var client = clients[index % clients.Count];
            var value = $"request-{index}";
            var response = await client.RequestAsync<EchoRequest, EchoRequest>(
                "stress",
                "echo",
                new EchoRequest(value));
            Assert.That(response.Value, Is.EqualTo(value));
          });

      await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));
    }
    finally
    {
      foreach (var client in clients)
        await client.DisposeAsync();
    }
  }

  sealed record EchoRequest(string Value);

  static SocketIdempotencyEntry CompletedIdempotencyEntry()
  {
    var entry = new SocketIdempotencyEntry();
    entry.TrySetResult(SocketResponse.Success());
    return entry;
  }

  static byte[] BuildFrameWithNegativeServiceLength()
  {
    using var output = new MemoryStream();
    Span<byte> buffer = stackalloc byte[8];
    WriteInt32(output, 39);
    WriteInt32(output, SocketFabricOptions.CurrentProtocolVersion);
    output.WriteByte((byte)SocketMessageKind.Ping);
    BinaryPrimitives.WriteInt64BigEndian(buffer, 1);
    output.Write(buffer[..8]);
    WriteInt32(output, 0);
    WriteInt32(output, 0);
    output.WriteByte(0);
    WriteInt32(output, 0);
    WriteInt32(output, 0);
    WriteInt32(output, 0);
    output.WriteByte(0);
    WriteInt32(output, -1);
    return output.ToArray();
  }

  static byte[] BuildFastRequestFrameWithTrailingByte()
  {
    using var payload = new MemoryStream();
    WriteFastRequestHeader(payload);
    WriteInt32(payload, 1);
    payload.WriteByte(42);
    payload.WriteByte(99);

    return BuildFrame(payload);
  }

  static byte[] BuildFastRequestFrameWithNegativePayloadLength()
  {
    using var payload = new MemoryStream();
    WriteFastRequestHeader(payload);
    WriteInt32(payload, -1);

    return BuildFrame(payload);
  }

  static void WriteFastRequestHeader(Stream stream)
  {
    WriteInt32(stream, SocketFabricOptions.CurrentProtocolVersion);
    stream.WriteByte((byte)SocketMessageKind.FastRequest);
    WriteInt64(stream, 1);
    WriteInt32(stream, 7);
    WriteInt32(stream, 0);
    WriteInt32(stream, 0);
    stream.WriteByte(0);
  }

  static byte[] BuildFrame(MemoryStream payload)
  {
    var bytes = payload.ToArray();
    using var output = new MemoryStream();
    WriteInt32(output, bytes.Length);
    output.Write(bytes);
    return output.ToArray();
  }

  static void WriteInt32(Stream stream, int value)
  {
    Span<byte> buffer = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(buffer, value);
    stream.Write(buffer);
  }

  static void WriteInt64(Stream stream, long value)
  {
    Span<byte> buffer = stackalloc byte[8];
    BinaryPrimitives.WriteInt64BigEndian(buffer, value);
    stream.Write(buffer);
  }

  static async ValueTask<SocketPeer> WaitForPeerAsync(SocketPeerRegistry registry, string nodeId)
  {
    var until = DateTimeOffset.UtcNow.AddSeconds(5);
    while (DateTimeOffset.UtcNow < until)
    {
      if (registry.TryGet(nodeId, out var peer))
        return peer;

      await Task.Delay(10);
    }

    Assert.Fail($"Peer '{nodeId}' was not registered.");
    return null;
  }

  static async ValueTask WaitForPeerCountAsync(SocketPeerRegistry registry, string nodeId, int count)
  {
    var until = DateTimeOffset.UtcNow.AddSeconds(5);
    while (DateTimeOffset.UtcNow < until)
    {
      if (registry.Snapshot(nodeId).Count == count)
        return;

      await Task.Delay(10);
    }

    Assert.Fail($"Peer '{nodeId}' did not reach count {count}.");
  }

  static async ValueTask RunRawNoAuthHandshakeAsync(Stream stream)
  {
    var reader = new SocketFrameReader(stream, 1024);
    var writer = new SocketFrameWriter(stream, 1024);
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "hello",
      Payload = JsonSocketMessageCodec.Shared.Serialize(new SocketHandshakeHello(
          SocketFabricOptions.CurrentProtocolVersion,
          "raw-client",
          Guid.NewGuid().ToString("N")))
    }, CancellationToken.None);

    var challenge = await reader.ReadAsync(CancellationToken.None);
    Assert.That(challenge.Operation, Is.EqualTo("challenge"));
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "proof",
      Payload = JsonSocketMessageCodec.Shared.Serialize(new SocketHandshakeProof(
          SocketFabricOptions.CurrentProtocolVersion,
          "raw-client",
          string.Empty))
    }, CancellationToken.None);

    var ready = await reader.ReadAsync(CancellationToken.None);
    Assert.That(ready.Operation, Is.EqualTo("ready"));
    Assert.That(ready.Ok, Is.True);
  }

  static IPEndPoint ReserveUnusedLoopbackEndPoint()
  {
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var endpoint = (IPEndPoint)listener.LocalEndpoint;
    listener.Stop();
    return endpoint;
  }

  sealed class DelegateSocketObserver : ISocketObserver
  {
    public Action<SocketPeer> OnPeerConnected { get; init; }
    public Action<IPEndPoint, Exception> OnPeerAcceptFailed { get; init; }
    public Action<IPEndPoint, Exception> OnListenerAcceptFailed { get; init; }
    public Action<IPEndPoint> OnClientConnectStarted { get; init; }
    public Action<IPEndPoint, Exception> OnClientConnectFailed { get; init; }
    public Action<IPEndPoint, TimeSpan> OnClientReconnectScheduled { get; init; }
    public Action<IPEndPoint, Exception> OnClientKeepConnectedLoopFailed { get; init; }
    public Action<SocketPeer, string, string, bool, string> OnRequestCompleted { get; init; }

    public void PeerConnected(SocketPeer peer)
    {
      OnPeerConnected?.Invoke(peer);
    }

    public void PeerAcceptFailed(IPEndPoint remoteEndPoint, Exception exception)
    {
      OnPeerAcceptFailed?.Invoke(remoteEndPoint, exception);
    }

    public void ListenerAcceptFailed(IPEndPoint localEndPoint, Exception exception)
    {
      OnListenerAcceptFailed?.Invoke(localEndPoint, exception);
    }

    public void ClientConnectStarted(IPEndPoint remoteEndPoint)
    {
      OnClientConnectStarted?.Invoke(remoteEndPoint);
    }

    public void ClientConnectFailed(IPEndPoint remoteEndPoint, Exception exception)
    {
      OnClientConnectFailed?.Invoke(remoteEndPoint, exception);
    }

    public void ClientReconnectScheduled(IPEndPoint remoteEndPoint, TimeSpan delay)
    {
      OnClientReconnectScheduled?.Invoke(remoteEndPoint, delay);
    }

    public void ClientKeepConnectedLoopFailed(IPEndPoint remoteEndPoint, Exception exception)
    {
      OnClientKeepConnectedLoopFailed?.Invoke(remoteEndPoint, exception);
    }

    public void RequestCompleted(
        SocketPeer peer,
        string service,
        string operation,
        bool succeeded,
        string errorCode)
    {
      OnRequestCompleted?.Invoke(peer, service, operation, succeeded, errorCode);
    }
  }

  sealed class ThrowingSocketObserver : ISocketObserver
  {
    public void PeerAcceptFailed(IPEndPoint remoteEndPoint, Exception exception)
    {
      throw new InvalidOperationException("observer failed");
    }
  }

  sealed class FailingReadStream(byte[] firstChunk) : Stream
  {
    bool ReadOnce;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
      get => throw new NotSupportedException();
      set => throw new NotSupportedException();
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
      if (ReadOnce)
        throw new InvalidOperationException("boom");

      ReadOnce = true;
      firstChunk.CopyTo(buffer);
      return ValueTask.FromResult(firstChunk.Length);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
      if (ReadOnce)
        throw new InvalidOperationException("boom");

      ReadOnce = true;
      Array.Copy(firstChunk, 0, buffer, offset, firstChunk.Length);
      return firstChunk.Length;
    }

    public override void Flush()
    {
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

  sealed class DelegateSocketAuthorizer : ISocketAuthorizer
  {
    public Func<SocketPeer, SocketRequest, CancellationToken, ValueTask<SocketAuthorizationResult>> OnRequest { get; init; }
    public Func<SocketPeer, SocketFastRequest, CancellationToken, ValueTask<SocketAuthorizationResult>> OnFastRequest { get; init; }
    public Func<SocketPeer, SocketStreamRequest, CancellationToken, ValueTask<SocketAuthorizationResult>> OnStream { get; init; }
    public Func<SocketPeer, SocketEvent, CancellationToken, ValueTask<SocketAuthorizationResult>> OnEvent { get; init; }

    public ValueTask<SocketAuthorizationResult> AuthorizeRequestAsync(
        SocketPeer peer,
        SocketRequest request,
        CancellationToken cancellationToken)
    {
      return OnRequest?.Invoke(peer, request, cancellationToken)
          ?? ValueTask.FromResult(SocketAuthorizationResult.Allowed);
    }

    public ValueTask<SocketAuthorizationResult> AuthorizeFastRequestAsync(
        SocketPeer peer,
        SocketFastRequest request,
        CancellationToken cancellationToken)
    {
      return OnFastRequest?.Invoke(peer, request, cancellationToken)
          ?? ValueTask.FromResult(SocketAuthorizationResult.Allowed);
    }

    public ValueTask<SocketAuthorizationResult> AuthorizeStreamAsync(
        SocketPeer peer,
        SocketStreamRequest request,
        CancellationToken cancellationToken)
    {
      return OnStream?.Invoke(peer, request, cancellationToken)
          ?? ValueTask.FromResult(SocketAuthorizationResult.Allowed);
    }

    public ValueTask<SocketAuthorizationResult> AuthorizeEventAsync(
        SocketPeer peer,
        SocketEvent socketEvent,
        CancellationToken cancellationToken)
    {
      return OnEvent?.Invoke(peer, socketEvent, cancellationToken)
          ?? ValueTask.FromResult(SocketAuthorizationResult.Allowed);
    }
  }

  static X509Certificate2 CreateTestCertificate()
  {
    using var rsa = RSA.Create(2048);
    var request = new CertificateRequest(
        "CN=puregate.socket",
        rsa,
        HashAlgorithmName.SHA256,
        RSASignaturePadding.Pkcs1);
    request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
    request.CertificateExtensions.Add(new X509KeyUsageExtension(
        X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
        false));
    request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
        new OidCollection
        {
          new("1.3.6.1.5.5.7.3.1")
        },
        false));
    request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
    var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
    subjectAlternativeNames.AddDnsName("puregate.socket");
    request.CertificateExtensions.Add(subjectAlternativeNames.Build());
    using var certificate = request.CreateSelfSigned(
        DateTimeOffset.UtcNow.AddDays(-1),
        DateTimeOffset.UtcNow.AddDays(1));
    return X509CertificateLoader.LoadPkcs12(
        certificate.Export(X509ContentType.Pkcs12),
        null,
        X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
  }

  sealed class DelegateSocketPeerHandler : ISocketPeerHandler
  {
    public Func<SocketPeer, CancellationToken, ValueTask> OnConnect { get; init; }
    public Func<SocketPeer, SocketRequest, CancellationToken, ValueTask<SocketResponse>> OnRequest { get; init; }
    public Func<SocketPeer, SocketFastRequest, CancellationToken, ValueTask<SocketResponse>> OnFastRequest { get; init; }
    public Func<SocketPeer, SocketStreamRequest, CancellationToken, ValueTask<SocketStreamResponse>> OnStream { get; init; }
    public Func<SocketPeer, SocketEvent, CancellationToken, ValueTask> OnEvent { get; init; }

    public ValueTask OnConnectedAsync(SocketPeer peer, CancellationToken cancellationToken)
    {
      return OnConnect?.Invoke(peer, cancellationToken) ?? ValueTask.CompletedTask;
    }

    public ValueTask<SocketResponse> HandleRequestAsync(
        SocketPeer peer,
        SocketRequest request,
        CancellationToken cancellationToken)
    {
      return OnRequest?.Invoke(peer, request, cancellationToken)
          ?? ValueTask.FromResult(SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "No handler."));
    }

    public ValueTask<SocketResponse> HandleFastRequestAsync(
        SocketPeer peer,
        SocketFastRequest request,
        CancellationToken cancellationToken)
    {
      return OnFastRequest?.Invoke(peer, request, cancellationToken)
          ?? ValueTask.FromResult(SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "No fast handler."));
    }

    public ValueTask<SocketStreamResponse> HandleStreamAsync(
        SocketPeer peer,
        SocketStreamRequest request,
        CancellationToken cancellationToken)
    {
      return OnStream?.Invoke(peer, request, cancellationToken)
          ?? ValueTask.FromResult(SocketStreamResponse.Failed(SocketProtocolError.UnsupportedOperation, "No stream handler."));
    }

    public ValueTask HandleEventAsync(
        SocketPeer peer,
        SocketEvent socketEvent,
        CancellationToken cancellationToken)
    {
      return OnEvent?.Invoke(peer, socketEvent, cancellationToken) ?? ValueTask.CompletedTask;
    }
  }
}
