using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Fabric;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

public sealed partial class SocketFabricTests
{
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
        Assert.That(async () => await call, Throws.InstanceOf<OperationCanceledException>());
      var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - started);

      Assert.That(Volatile.Read(ref attempts), Is.EqualTo(1));
      // One after another, the callers would have taken callers x HandshakeTimeout = 4 s.
      Assert.That(elapsed, Is.LessThan(TimeSpan.FromSeconds(3)));

      // The next call after the failure starts a new attempt.
      Assert.That(async () => await client.GetPeerAsync(), Throws.InstanceOf<OperationCanceledException>());
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
