using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using PureGate.Socket.Protocol;
using PureGate.Socket.Security;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Hardening tests: limits that hold before a peer has authenticated, identity binding, and a soak test
// that checks per-connection state and pooled buffers drain after sustained chaotic traffic.
public sealed partial class SocketFabricTests
{
  [Test]
  [Category("Hardening")]
  public async Task PreAuth_HandshakeFrameOverTheHandshakeLimitIsRejected()
  {
    var failure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          Observer = new DelegateSocketObserver { OnPeerAcceptFailed = (_, exception) => failure.TrySetResult(exception) }
        },
        new DelegateSocketPeerHandler());

    using var tcp = new TcpClient();
    await tcp.ConnectAsync(listener.EndPoint);
    var stream = tcp.GetStream();
    // Well under MaxFrameSizeBytes (1 MB) but over MaxHandshakeFrameBytes (16 KB).
    var header = new byte[4];
    BinaryPrimitives.WriteInt32BigEndian(header, 20_000);
    await stream.WriteAsync(header);
    await stream.WriteAsync(new byte[256]);

    var exception = await failure.Task.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.That(exception, Is.InstanceOf<SocketProtocolException>());
    Assert.That(((SocketProtocolException)exception).ErrorCode, Is.EqualTo(SocketProtocolError.PayloadTooLarge));
    Assert.That(await IsClosedByPeerAsync(stream), Is.True, "the listener should close the connection");
  }

  [Test]
  [Category("Hardening")]
  public async Task PreAuth_ConnectionsPastMaxPendingHandshakesAreClosedAtOnce()
  {
    var rejected = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          MaxPendingHandshakes = 2,
          HandshakeTimeout = TimeSpan.FromSeconds(30),
          Observer = new DelegateSocketObserver
          {
            OnPeerAcceptFailed = (_, exception) =>
            {
              if (exception is SocketProtocolException { ErrorCode: SocketProtocolError.Overloaded })
                Interlocked.Increment(ref rejected);
            }
          }
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload })
        });

    // Two connections that never say hello hold both handshake slots.
    using var silentA = new TcpClient();
    using var silentB = new TcpClient();
    await silentA.ConnectAsync(listener.EndPoint);
    await silentB.ConnectAsync(listener.EndPoint);

    using var extra = new TcpClient();
    await extra.ConnectAsync(listener.EndPoint);
    Assert.That(await IsClosedByPeerAsync(extra.GetStream()), Is.True, "a third handshake should be refused");
    Assert.That(Volatile.Read(ref rejected), Is.EqualTo(1));

    // Once the silent connections go away their slots are free again.
    silentA.Dispose();
    silentB.Dispose();
    await using var peer = await ConnectWithRetryAsync(listener.EndPoint, TimeSpan.FromSeconds(5));
    var result = await peer.RequestAsync("test", "echo", [1, 2, 3], TimeSpan.FromSeconds(5));
    Assert.That(result.Succeeded, Is.True, result.ErrorCode);
  }

  [Test]
  [Category("Hardening")]
  public async Task PreAuth_MaxConnectionsRejectsConnectionsPastTheLimitUntilOneCloses()
  {
    var disconnected = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          MaxConnections = 2,
          Observer = new DelegateSocketObserver { OnPeerDisconnected = (_, _) => Interlocked.Increment(ref disconnected) }
        },
        new DelegateSocketPeerHandler());

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    var first = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await using var second = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await Assert.CatchAsync(async () => await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler()));

    await first.DisposeAsync();
    await WaitUntilAsync(() => Volatile.Read(ref disconnected) > 0, TimeSpan.FromSeconds(5));
    await using var third = await ConnectWithRetryAsync(listener.EndPoint, TimeSpan.FromSeconds(5));
    Assert.That(third.RemoteNodeId, Is.EqualTo("server"));
  }

  [Test]
  [Category("Hardening")]
  public async Task Hmac_PerNodeSecretsStopOneNodeFromPosingAsAnother()
  {
    var ab = RandomNumberGenerator.GetBytes(32);
    var bc = RandomNumberGenerator.GetBytes(32);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "B",
          Authenticator = new HmacSocketAuthenticator(remote => remote switch { "A" => ab, "C" => bc, _ => null })
        },
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, _, _) => ValueTask.FromResult(new SocketResponse
          {
            Succeeded = true,
            Payload = Encoding.UTF8.GetBytes(peer.RemoteNodeId)
          })
        });

    async ValueTask<SocketPeer> ConnectAs(string nodeId, byte[] secretWithB)
    {
      return await new SocketConnector(new SocketFabricOptions
      {
        NodeId = nodeId,
        Authenticator = new HmacSocketAuthenticator(remote => remote == "B" ? secretWithB : null)
      }).ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    }

    await using (var a = await ConnectAs("A", ab))
      Assert.That(Encoding.UTF8.GetString((await a.RequestAsync("who", "am-i", [], TimeSpan.FromSeconds(5))).Payload.Span), Is.EqualTo("A"));
    await using (var c = await ConnectAs("C", bc))
      Assert.That(Encoding.UTF8.GetString((await c.RequestAsync("who", "am-i", [], TimeSpan.FromSeconds(5))).Payload.Span), Is.EqualTo("C"));

    // C holds only the C-B secret, so it cannot authenticate as A.
    var impersonation = await Assert.ThrowsAsync<SocketProtocolException>(async () => await ConnectAs("A", bc));
    Assert.That(impersonation.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));

    // A node B has no secret for is turned away the same way.
    var unknown = await Assert.ThrowsAsync<SocketProtocolException>(async () => await ConnectAs("D", RandomNumberGenerator.GetBytes(32)));
    Assert.That(unknown.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));

    // A client whose lookup has no secret for the server refuses it.
    var noSecret = await Assert.ThrowsAsync<SocketProtocolException>(async () => await ConnectAs("A", null));
    Assert.That(noSecret.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
  }

  [Test]
  [Category("Hardening")]
  public async Task Tls_NodeIdsAreBoundToCertificatesOnBothSides()
  {
    using var serverCertificate = CreateNodeCertificate("server");
    using var aliceCertificate = CreateNodeCertificate("alice");

    TlsSocketStreamFilter ServerFilter(X509Certificate2 certificate)
    {
      return TlsSocketStreamFilter.Create(
          certificate,
          "server",
          clientCertificateValidation: (_, _, _, _) => true,
          requireClientCertificate: true,
          bindNodeIdToCertificate: true);
    }

    TlsSocketStreamFilter ClientFilter(X509Certificate2 clientCertificate)
    {
      return TlsSocketStreamFilter.Create(
          serverCertificate,
          "server",
          serverCertificateValidation: (_, _, _, _) => true,
          clientCertificates: clientCertificate is null ? null : [clientCertificate],
          bindNodeIdToCertificate: true);
    }

    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", StreamFilter = ServerFilter(serverCertificate) },
        new DelegateSocketPeerHandler());

    await using (var alice = await new SocketConnector(new SocketFabricOptions { NodeId = "alice", StreamFilter = ClientFilter(aliceCertificate) })
        .ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler()))
    {
      Assert.That(alice.RemoteNodeId, Is.EqualTo("server"));
    }

    // Alice's certificate does not name "mallory".
    var wrongName = await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await new SocketConnector(new SocketFabricOptions { NodeId = "mallory", StreamFilter = ClientFilter(aliceCertificate) })
            .ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler()));
    Assert.That(wrongName.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));

    // No client certificate at all.
    await Assert.CatchAsync(async () =>
        await new SocketConnector(new SocketFabricOptions { NodeId = "alice", StreamFilter = ClientFilter(null) })
            .ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler()));

    // The client checks the server too: a server whose certificate does not name its node ID is refused.
    await using var impostor = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "impostor", StreamFilter = ServerFilter(serverCertificate) },
        new DelegateSocketPeerHandler());
    var wrongServer = await Assert.ThrowsAsync<SocketProtocolException>(async () =>
        await new SocketConnector(new SocketFabricOptions { NodeId = "alice", StreamFilter = ClientFilter(aliceCertificate) })
            .ConnectAsync(impostor.EndPoint, new DelegateSocketPeerHandler()));
    Assert.That(wrongServer.ErrorCode, Is.EqualTo(SocketProtocolError.Unauthorized));
  }

  [Test]
  [Category("Hardening")]
  public async Task Idempotency_KeysAreScopedToTheAuthenticatedNode()
  {
    var calls = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, _, _) =>
          {
            Interlocked.Increment(ref calls);
            return ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = Encoding.UTF8.GetBytes(peer.RemoteNodeId) });
          }
        });

    await using var alice = await new SocketConnector(new SocketFabricOptions { NodeId = "alice" }).ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await using var bob = await new SocketConnector(new SocketFabricOptions { NodeId = "bob" }).ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    var aliceFirst = await alice.RequestAsync("orders", "create", [], TimeSpan.FromSeconds(5), idempotencyKey: "order-1");
    var aliceRetry = await alice.RequestAsync("orders", "create", [], TimeSpan.FromSeconds(5), idempotencyKey: "order-1");
    var bobSameKey = await bob.RequestAsync("orders", "create", [], TimeSpan.FromSeconds(5), idempotencyKey: "order-1");

    Assert.That(Encoding.UTF8.GetString(aliceFirst.Payload.Span), Is.EqualTo("alice"));
    Assert.That(Encoding.UTF8.GetString(aliceRetry.Payload.Span), Is.EqualTo("alice"));
    Assert.That(Encoding.UTF8.GetString(bobSameKey.Payload.Span), Is.EqualTo("bob"), "bob must not see alice's cached response");
    Assert.That(calls, Is.EqualTo(2), "alice's retry is served from the cache; bob's call is new");
  }

  // Sustained mixed traffic (small and chunked requests, streams read fully or abandoned, events,
  // timeouts and cancellations) through a proxy that fragments and resets connections. Afterwards every
  // connection's state must drain and every pooled buffer must be returned.
  // Runs for PUREGATE_SOAK_SECONDS (default 20); the manual `soak` workflow runs it for an hour by default.
  [Test]
  [Explicit("Long-running; run with --filter Category=Soak")]
  [Category("Soak")]
  public async Task Soak_MixedChaoticTrafficLeavesNoStateOrBuffersBehind()
  {
    var seconds = int.TryParse(Environment.GetEnvironmentVariable("PUREGATE_SOAK_SECONDS"), out var configured) && configured > 0
        ? configured
        : 20;
    var baselineBuffers = SocketPayload.OutstandingPooledBuffers;
    var serverPeers = new ConcurrentBag<SocketPeer>();
    var connected = 0;
    var disconnected = 0;
    var events = 0;
    var serverOptions = new SocketFabricOptions
    {
      NodeId = "server",
      MaxFrameSizeBytes = 64 * 1024,
      Observer = new DelegateSocketObserver
      {
        OnPeerConnected = peer =>
        {
          serverPeers.Add(peer);
          Interlocked.Increment(ref connected);
        },
        OnPeerDisconnected = (_, _) => Interlocked.Increment(ref disconnected)
      }
    };
    await using var listener = await StartListenerAsync(serverOptions, new DelegateSocketPeerHandler
    {
      OnRequest = async (_, request, cancellationToken) =>
      {
        if (request.Operation == "slow")
          await Task.Delay(50, cancellationToken);
        return new SocketResponse { Succeeded = true, Payload = request.Payload.ToArray() };
      },
      OnStream = (_, request, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(BinaryPrimitives.ReadInt32BigEndian(request.Payload.Span)))),
      OnEvent = (_, _, _) =>
      {
        Interlocked.Increment(ref events);
        return ValueTask.CompletedTask;
      }
    });
    await using var proxy = ChaosProxy.Start(listener.EndPoint, new ChaosOptions
    {
      MaxFragmentBytes = 1500,
      DelayOneIn = 2000,
      ResetAfterBytes = (200_000, 8_000_000),
      Seed = FuzzSeed
    });

    var clientOptions = new SocketFabricOptions { NodeId = "client", MaxFrameSizeBytes = 64 * 1024, StreamWindowBytes = 128 * 1024 };
    var connector = new SocketConnector(clientOptions);
    var unexpected = new ConcurrentQueue<string>();
    var clientPeers = new ConcurrentBag<SocketPeer>();
    var operations = 0L;
    var deadline = DateTime.UtcNow.AddSeconds(seconds);

    await Task.WhenAll(Enumerable.Range(0, 8).Select(worker => Task.Run(async () =>
    {
      var random = new Random(FuzzSeed + worker);
      SocketPeer peer = null;
      while (DateTime.UtcNow < deadline && unexpected.Count < 20)
      {
        try
        {
          if (peer is null || peer.Completion.IsCompleted)
          {
            if (peer is not null)
              await peer.DisposeAsync();
            peer = await connector.ConnectAsync(proxy.EndPoint, new DelegateSocketPeerHandler());
            clientPeers.Add(peer);
          }

          await RunSoakOperationAsync(peer, random, unexpected);
          Interlocked.Increment(ref operations);
        }
        catch (Exception ex) when (IsCleanFailure(ex))
        {
        }
        catch (Exception ex)
        {
          unexpected.Enqueue($"worker {worker}: {ex.GetType().Name}: {ex.Message}");
        }
      }
    })));

    Assert.That(unexpected, Is.Empty, string.Join(Environment.NewLine, unexpected));
    TestContext.Out.WriteLine($"soak: {seconds}s, {operations} operations, {proxy.Resets} resets, {connected} connections, {events} events");

    // Traffic has stopped: every live connection, on both sides, must drain to idle.
    var live = clientPeers.Concat(serverPeers).Where(peer => !peer.Completion.IsCompleted).ToList();
    await WaitUntilAsync(() => live.All(peer => peer.Completion.IsCompleted || peer.Diagnostics.IsIdle), TimeSpan.FromSeconds(15));
    var busy = live.Where(peer => !peer.Completion.IsCompleted && !peer.Diagnostics.IsIdle).Select(peer => $"{peer.LocalNodeId}: {peer.Diagnostics}").ToList();
    Assert.That(busy, Is.Empty, string.Join(Environment.NewLine, busy));

    foreach (var peer in clientPeers)
      await peer.DisposeAsync();
    await WaitUntilAsync(() => Volatile.Read(ref disconnected) == Volatile.Read(ref connected), TimeSpan.FromSeconds(15));
    Assert.That(disconnected, Is.EqualTo(connected), "every server-side connection should close");

    await WaitUntilAsync(() => SocketPayload.OutstandingPooledBuffers <= baselineBuffers, TimeSpan.FromSeconds(15));
    Assert.That(SocketPayload.OutstandingPooledBuffers, Is.LessThanOrEqualTo(baselineBuffers), "pooled buffers leaked");
    Assert.That(operations, Is.GreaterThan(seconds * 10), "the soak should keep traffic flowing despite resets");
  }

  static async ValueTask RunSoakOperationAsync(SocketPeer peer, Random random, ConcurrentQueue<string> unexpected)
  {
    var timeout = TimeSpan.FromSeconds(10);
    switch (random.Next(10))
    {
      case < 4:
      {
        var payload = RandomBytes(random, random.Next(0, 4096));
        CheckEcho(await peer.RequestAsync("soak", "echo", payload, timeout), payload, unexpected);
        break;
      }
      case 4:
      {
        // Larger than MaxFrameSizeBytes, so it travels in chunks.
        var payload = RandomBytes(random, random.Next(64 * 1024, 300 * 1024));
        CheckEcho(await peer.RequestAsync("soak", "echo", payload, timeout), payload, unexpected);
        break;
      }
      case 5:
      case 6:
      {
        var length = random.Next(0, 1_000_000);
        var readAll = random.Next(2) == 0;
        var request = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(request, length);
        await using var stream = await peer.OpenStreamAsync("soak", "pattern", request, timeout);
        var buffer = new byte[16 * 1024];
        long position = 0;
        var stopAt = readAll ? long.MaxValue : random.Next(0, length + 1);
        int read;
        while (position < stopAt && (read = await stream.ReadAsync(buffer)) > 0)
        {
          for (var i = 0; i < read; i++)
          {
            if (buffer[i] != PatternStream.ByteAt(position + i))
            {
              unexpected.Enqueue($"stream byte {position + i} is wrong");
              return;
            }
          }

          position += read;
        }

        if (readAll && position != length)
          unexpected.Enqueue($"stream ended at {position} of {length} bytes");
        break;
      }
      case 7:
        await peer.SendEventAsync("soak", "event", RandomBytes(random, random.Next(0, 2048)));
        break;
      case 8:
      {
        // Times out before the slow handler answers.
        var result = await peer.RequestAsync("soak", "slow", [], TimeSpan.FromMilliseconds(5));
        if (!result.Succeeded && !CleanFailureCodes.Contains(result.ErrorCode))
          unexpected.Enqueue($"slow request: {result.ErrorCode} {result.ErrorMessage}");
        break;
      }
      default:
      {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(random.Next(1, 30)));
        var result = await peer.RequestAsync("soak", "slow", [], timeout, cancellationToken: cancellation.Token);
        if (!result.Succeeded && !CleanFailureCodes.Contains(result.ErrorCode))
          unexpected.Enqueue($"cancelled request: {result.ErrorCode} {result.ErrorMessage}");
        break;
      }
    }
  }

  static void CheckEcho(SocketRequestResult result, byte[] payload, ConcurrentQueue<string> unexpected)
  {
    if (result.Succeeded)
    {
      if (!result.Payload.Span.SequenceEqual(payload))
        unexpected.Enqueue($"echo of {payload.Length} bytes came back as {result.Payload.Length} different bytes");
    }
    else if (!CleanFailureCodes.Contains(result.ErrorCode))
    {
      unexpected.Enqueue($"echo: {result.ErrorCode} {result.ErrorMessage}");
    }
  }

  static bool IsCleanFailure(Exception exception)
  {
    return exception is SocketProtocolException protocol && CleanFailureCodes.Contains(protocol.ErrorCode)
        || exception is IOException or SocketException or OperationCanceledException or ObjectDisposedException;
  }

  static async ValueTask<SocketPeer> ConnectWithRetryAsync(IPEndPoint endpoint, TimeSpan timeout)
  {
    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    var until = DateTime.UtcNow + timeout;
    while (true)
    {
      try
      {
        return await connector.ConnectAsync(endpoint, new DelegateSocketPeerHandler());
      }
      catch (Exception) when (DateTime.UtcNow < until)
      {
        await Task.Delay(50);
      }
    }
  }

  // True when the other side closes (FIN or RST) within a few seconds.
  static async ValueTask<bool> IsClosedByPeerAsync(NetworkStream stream)
  {
    try
    {
      var buffer = new byte[256];
      using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      while (await stream.ReadAsync(buffer, timeout.Token) > 0)
      {
      }

      return true;
    }
    catch (IOException)
    {
      return true;
    }
    catch (OperationCanceledException)
    {
      return false;
    }
  }

  static X509Certificate2 CreateNodeCertificate(string nodeId)
  {
    using var rsa = RSA.Create(2048);
    var request = new CertificateRequest($"CN={nodeId}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
    request.CertificateExtensions.Add(new X509KeyUsageExtension(
        X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
        false));
    request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
        new OidCollection
        {
          new("1.3.6.1.5.5.7.3.1"),
          new("1.3.6.1.5.5.7.3.2")
        },
        false));
    var names = new SubjectAlternativeNameBuilder();
    names.AddDnsName(nodeId);
    request.CertificateExtensions.Add(names.Build());
    using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    return X509CertificateLoader.LoadPkcs12(
        certificate.Export(X509ContentType.Pkcs12),
        null,
        X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
  }
}
