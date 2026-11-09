using System.Net;
using System.Net.Sockets;
using NUnit.Framework.Constraints;
using PureGate.Socket.Fabric;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

public sealed partial class SocketFabricTests
{
  // What a connect that HandshakeTimeout ended throws, at any step.
  static IResolveConstraint TimedOut => Throws.InstanceOf<SocketException>()
      .With.Property(nameof(SocketException.SocketErrorCode)).EqualTo(SocketError.TimedOut);

  // Callers that need a connection while a connect is running share that attempt: a dead endpoint costs one
  // attempt however many callers there are, instead of one attempt per caller, one after another.
  [Test]
  public async Task SocketClient_CallersShareAFailingConnect()
  {
    // Accepts TCP connections and never answers the handshake, so every attempt takes HandshakeTimeout.
    var silent = new TcpListener(IPAddress.Loopback, 0);
    silent.Start();
    var held = new List<TcpClient>();
    using var stop = new CancellationTokenSource();
    var accepting = Task.Run(async () =>
    {
      try
      {
        while (true)
          held.Add(await silent.AcceptTcpClientAsync(stop.Token));
      }
      catch (Exception) when (stop.IsCancellationRequested)
      {
      }
    });

    var attempts = 0;
    await using var client = new SocketClient(
        (IPEndPoint)silent.LocalEndpoint,
        new DelegateSocketPeerHandler(),
        new SocketFabricOptions
        {
          NodeId = "client",
          HandshakeTimeout = TimeSpan.FromMilliseconds(500),
          Observer = new DelegateSocketObserver { OnClientConnectStarted = _ => Interlocked.Increment(ref attempts) }
        });

    try
    {
      const int callers = 8;
      var started = Environment.TickCount64;
      var calls = Enumerable.Range(0, callers).Select(_ => client.GetPeerAsync().AsTask()).ToArray();
      foreach (var call in calls)
        Assert.That(async () => await call, TimedOut);
      var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - started);

      Assert.That(Volatile.Read(ref attempts), Is.EqualTo(1));
      // One after another, the callers would have taken callers x HandshakeTimeout = 4 s.
      Assert.That(elapsed, Is.LessThan(TimeSpan.FromSeconds(3)));

      // The next call after the failure starts a new attempt.
      Assert.That(async () => await client.GetPeerAsync(), TimedOut);
      Assert.That(Volatile.Read(ref attempts), Is.EqualTo(2));
    }
    finally
    {
      stop.Cancel();
      silent.Stop();
      await accepting;
      foreach (var connection in held)
        connection.Dispose();
    }
  }

  // Drops a SYN without answering on most networks. A network that answers instead (no route, an ICMP
  // error) makes the tests that use it inconclusive.
  static readonly IPEndPoint SilentEndPoint = new(IPAddress.Parse("10.255.255.1"), 9);

  // Windows used to send the SYN again for about 2 s before reporting the refusal.
  [Test]
  public async Task Connector_RefusedConnectFailsAtOnce()
  {
    var probe = new TcpListener(IPAddress.Loopback, 0);
    probe.Start();
    var closed = (IPEndPoint)probe.LocalEndpoint;
    probe.Stop();
    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });

    var started = Environment.TickCount64;
    var error = await Assert.ThrowsAsync<SocketException>(async () => await connector.ConnectAsync(closed, new DelegateSocketPeerHandler()));
    var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - started);

    Assert.That(error.SocketErrorCode, Is.EqualTo(SocketError.ConnectionRefused));
    Assert.That(elapsed, Is.LessThan(TimeSpan.FromSeconds(1)));
  }

  // A node that never answers is unreachable, not a cancellation. On Windows the connect keeps trying past
  // its first ~1 s attempt, so a lost SYN is sent again, until HandshakeTimeout.
  [Test]
  public async Task Connector_SilentEndPointTimesOutAsSocketException()
  {
    var timeout = TimeSpan.FromMilliseconds(2500);
    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client", HandshakeTimeout = timeout });

    var started = Environment.TickCount64;
    SocketException error = null;
    try
    {
      await using var peer = await connector.ConnectAsync(SilentEndPoint, new DelegateSocketPeerHandler());
      Assert.Fail("Connected to an endpoint that should not answer.");
    }
    catch (SocketException ex)
    {
      error = ex;
    }
    var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - started);

    if (error.SocketErrorCode != SocketError.TimedOut)
      Assert.Inconclusive($"The network answered {SilentEndPoint}: {error.SocketErrorCode}.");
    Assert.That(elapsed, Is.GreaterThanOrEqualTo(timeout - TimeSpan.FromMilliseconds(100)));
    Assert.That(elapsed, Is.LessThan(timeout + TimeSpan.FromSeconds(2)));
  }

  // The caller's own token still throws OperationCanceledException, also while the connect is being retried.
  [Test]
  public async Task Connector_CallerCancellationThrowsOperationCanceled()
  {
    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client", HandshakeTimeout = TimeSpan.FromSeconds(5) });
    using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(1500));

    try
    {
      await using var peer = await connector.ConnectAsync(SilentEndPoint, new DelegateSocketPeerHandler(), cancel.Token);
      Assert.Fail("Connected to an endpoint that should not answer.");
    }
    catch (OperationCanceledException)
    {
    }
    catch (SocketException ex) when (ex.SocketErrorCode != SocketError.TimedOut)
    {
      Assert.Inconclusive($"The network answered {SilentEndPoint}: {ex.SocketErrorCode}.");
    }
  }

  // A handler of the client's connection that calls the client after that connection closed: the client
  // replaces the connection without waiting for the old one's handlers, the caller among them.
  [Test]
  public async Task SocketClient_HandlerOfClosedConnectionCanReconnect()
  {
    var connections = 0;
    var handling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnConnect = async (peer, cancellationToken) =>
      {
        // The first connection starts the client's handler, then closes while it runs. (Closing at once
        // could drop the event before a handler took it.)
        if (Interlocked.Increment(ref connections) != 1)
          return;
        await peer.SendEventAsync("test", "go", [1], cancellationToken);
        await handling.Task.WaitAsync(OrderTimeout, cancellationToken);
        await peer.CloseGracefullyAsync(cancellationToken);
      },
      OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload })
    });

    var clientSource = new TaskCompletionSource<SocketClient>(TaskCreationOptions.RunContinuationsAsynchronously);
    var result = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var client = new SocketClient(
        listener.EndPoint,
        new DelegateSocketPeerHandler
        {
          OnEvent = async (peer, _, _) =>
          {
            try
            {
              handling.TrySetResult();
              await peer.Completion.WaitAsync(OrderTimeout);
              var self = await clientSource.Task;
              var response = await self.RequestAsync("test", "echo", new EchoRequest("again"));
              result.TrySetResult(response.Succeeded);
            }
            catch (Exception ex)
            {
              result.TrySetException(ex);
            }
          }
        },
        new SocketFabricOptions { NodeId = "client" });
    clientSource.TrySetResult(client);
    await client.GetPeerAsync();

    Assert.That(await result.Task.WaitAsync(OrderTimeout), Is.True);
  }
}
