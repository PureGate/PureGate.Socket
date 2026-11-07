using System.Collections.Concurrent;
using System.Net;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// The order of a connection's handler callbacks: OnConnectedAsync returns before any request, stream or
// event handler runs, and before OnDisconnectedAsync.
public sealed partial class SocketFabricTests
{
  static readonly TimeSpan OrderTimeout = TimeSpan.FromSeconds(10);

  [Test]
  public async Task Handlers_RunOnlyAfterOnConnectedAsyncReturns()
  {
    var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var eventSawConnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnConnect = async (_, _) =>
      {
        await gate.Task;
        connected.TrySetResult();
      },
      OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse
      {
        Succeeded = connected.Task.IsCompleted,
        Payload = request.Payload
      }),
      OnEvent = (_, _, _) =>
      {
        eventSawConnected.TrySetResult(connected.Task.IsCompleted);
        return ValueTask.CompletedTask;
      }
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await client.SendEventAsync("test", "event", [1]);
    var request = client.RequestAsync("test", "echo", [2], OrderTimeout).AsTask();

    // Held while the callback runs.
    await Task.Delay(200);
    Assert.That(request.IsCompleted, Is.False);
    Assert.That(eventSawConnected.Task.IsCompleted, Is.False);

    gate.TrySetResult();
    var response = await request.WaitAsync(OrderTimeout);
    Assert.That(response.Succeeded, Is.True);
    Assert.That(await eventSawConnected.Task.WaitAsync(OrderTimeout), Is.True);
  }

  // The read loop keeps running during the callback, so responses and credits reach it.
  [Test]
  public async Task OnConnectedAsync_CanSendAndWaitForResponses()
  {
    var result = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    var allEvents = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var events = 0;
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnConnect = async (peer, cancellationToken) =>
      {
        try
        {
          // Far more events than the client's credit allows, so the callback needs credit to come back.
          for (var i = 0; i < 64; i++)
            await peer.SendEventAsync("test", "event", [1], cancellationToken);
          var response = await peer.RequestAsync("test", "hello", [2], OrderTimeout, cancellationToken: cancellationToken);
          result.TrySetResult(response.Succeeded && response.Payload.Span.SequenceEqual(new byte[] { 2 }));
        }
        catch (Exception ex)
        {
          result.TrySetException(ex);
        }
      }
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client", MaxQueuedHandlers = 2 });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler
    {
      OnEvent = (_, _, _) =>
      {
        if (Interlocked.Increment(ref events) == 64)
          allEvents.TrySetResult();
        return ValueTask.CompletedTask;
      },
      OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload })
    });

    Assert.That(await result.Task.WaitAsync(OrderTimeout), Is.True);
    await allEvents.Task.WaitAsync(OrderTimeout);
  }

  [Test]
  public async Task OnDisconnectedAsync_WaitsForOnConnectedAsyncWhenClosedDuringIt()
  {
    var order = new ConcurrentQueue<string>();
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var disconnects = 0;
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnConnect = async (_, cancellationToken) =>
      {
        entered.TrySetResult();
        try
        {
          await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }

        // Still busy for a moment after the close cancelled it.
        await Task.Delay(100);
        order.Enqueue("connected returned");
      },
      OnDisconnect = (_, _) =>
      {
        Interlocked.Increment(ref disconnects);
        order.Enqueue("disconnected");
        disconnected.TrySetResult();
        return ValueTask.CompletedTask;
      }
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await entered.Task.WaitAsync(OrderTimeout);
    await client.DisposeAsync();

    await disconnected.Task.WaitAsync(OrderTimeout);
    await Task.Delay(100);
    Assert.That(order, Is.EqualTo(new[] { "connected returned", "disconnected" }));
    Assert.That(Volatile.Read(ref disconnects), Is.EqualTo(1));
  }

  // Disposing the peer from inside its own OnConnectedAsync, for example to reject it, neither waits for
  // the callback that is running nor lets OnDisconnectedAsync overtake it.
  [Test]
  public async Task OnConnectedAsync_CanDisposeItsPeer()
  {
    var order = new ConcurrentQueue<string>();
    var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnConnect = async (peer, _) =>
      {
        await peer.DisposeAsync();
        order.Enqueue("connected returned");
      },
      OnDisconnect = (_, _) =>
      {
        order.Enqueue("disconnected");
        disconnected.TrySetResult();
        return ValueTask.CompletedTask;
      }
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());

    await disconnected.Task.WaitAsync(OrderTimeout);
    await client.Completion.WaitAsync(OrderTimeout);
    Assert.That(order, Is.EqualTo(new[] { "connected returned", "disconnected" }));
  }

  // The read loop that saw the remote close runs OnDisconnectedAsync; disposing the peer from there must not
  // wait for that read loop.
  [Test]
  public async Task OnDisconnectedAsync_CanDisposeItsPeer()
  {
    var disposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnDisconnect = async (peer, _) =>
      {
        await peer.DisposeAsync();
        disposed.TrySetResult();
      }
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    await client.DisposeAsync();

    await disposed.Task.WaitAsync(OrderTimeout);
  }
}
