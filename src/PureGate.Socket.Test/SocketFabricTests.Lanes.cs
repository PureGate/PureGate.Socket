using System.Net;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Handler lanes (SocketFabricOptions.HandlerLanes): chosen operations run on a pool of their own.
public sealed partial class SocketFabricTests
{
  // Each side's "outer" handler calls the other side's "inner" operation and waits for it. With two general
  // handlers per side, both sides fill up with outer calls, and every inner call queues behind them.
  [Test]
  public async Task HandlerLanes_BreakCallbackDeadlock()
  {
    Assert.That(await CallbackStormFailuresAsync(withLanes: false), Is.GreaterThan(0));
    Assert.That(await CallbackStormFailuresAsync(withLanes: true), Is.Zero);
  }

  static async Task<int> CallbackStormFailuresAsync(bool withLanes)
  {
    IReadOnlyList<SocketHandlerLane> lanes = withLanes ? [new SocketHandlerLane("test", "inner", 2)] : [];
    SocketFabricOptions Options(string nodeId) => new() { NodeId = nodeId, MaxConcurrentHandlers = 2, HandlerLanes = lanes };

    // The first outer calls wait until both sides' general handlers are all taken, so the cycle forms on
    // every run, however the calls are scheduled.
    var entered = 0;
    var allTaken = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    Func<SocketPeer, SocketRequest, CancellationToken, ValueTask<SocketResponse>> onRequest = async (peer, request, cancellationToken) =>
    {
      if (request.Operation == "inner")
        return new SocketResponse { Succeeded = true };

      if (Interlocked.Increment(ref entered) == 4)
        allTaken.TrySetResult();
      await allTaken.Task.WaitAsync(OrderTimeout, cancellationToken);
      var inner = await peer.RequestAsync("test", "inner", [1], TimeSpan.FromMilliseconds(500), cancellationToken: cancellationToken);
      return inner.Succeeded
          ? new SocketResponse { Succeeded = true }
          : SocketResponse.Failed(inner.ErrorCode, "The inner call failed.");
    };

    var serverPeer = new TaskCompletionSource<SocketPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(Options("a"));
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnConnect = (peer, _) =>
      {
        serverPeer.TrySetResult(peer);
        return ValueTask.CompletedTask;
      },
      OnRequest = onRequest
    });
    await using var b = await new SocketConnector(Options("b")).ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler { OnRequest = onRequest });
    var a = await serverPeer.Task.WaitAsync(OrderTimeout);

    var calls = Enumerable.Range(0, 4)
        .SelectMany(_ => new[] { a, b })
        .Select(peer => peer.RequestAsync("test", "outer", [1], TimeSpan.FromSeconds(10)).AsTask())
        .ToArray();
    var results = await Task.WhenAll(calls);
    return results.Count(result => !result.Succeeded);
  }

  [Test]
  public async Task HandlerLanes_KeepTheirOwnLimitAndSlots()
  {
    var generalGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var laneGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var generalEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var laneEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var laneRunning = 0;
    var laneMaxRunning = 0;
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxConcurrentHandlers = 1,
      HandlerLanes = [new SocketHandlerLane("lane", MaxConcurrentHandlers: 1)]
    });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnRequest = async (_, request, cancellationToken) =>
      {
        switch (request.Service, request.Operation)
        {
          case ("general", "block"):
            generalEntered.TrySetResult();
            await generalGate.Task.WaitAsync(OrderTimeout, cancellationToken);
            break;
          case ("lane", "block"):
            var running = Interlocked.Increment(ref laneRunning);
            InterlockedMax(ref laneMaxRunning, running);
            laneEntered.TrySetResult();
            await laneGate.Task.WaitAsync(OrderTimeout, cancellationToken);
            Interlocked.Decrement(ref laneRunning);
            break;
        }

        return new SocketResponse { Succeeded = true };
      }
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    var timeout = TimeSpan.FromSeconds(5);

    // The only general handler is taken; the lane still runs.
    var generalBlock = client.RequestAsync("general", "block", [1], OrderTimeout).AsTask();
    await generalEntered.Task.WaitAsync(OrderTimeout);
    Assert.That((await client.RequestAsync("lane", "quick", [1], timeout)).Succeeded, Is.True);

    // The lane's only handler is taken: a second lane request waits for it.
    var laneFirst = client.RequestAsync("lane", "block", [1], OrderTimeout).AsTask();
    await laneEntered.Task.WaitAsync(OrderTimeout);
    var laneSecond = client.RequestAsync("lane", "block", [1], OrderTimeout).AsTask();
    await Task.Delay(200);
    Assert.That(laneSecond.IsCompleted, Is.False);

    // The lane is full; the general handlers still run.
    generalGate.TrySetResult();
    Assert.That((await generalBlock).Succeeded, Is.True);
    Assert.That((await client.RequestAsync("general", "quick", [1], timeout)).Succeeded, Is.True);

    laneGate.TrySetResult();
    Assert.That((await laneFirst).Succeeded, Is.True);
    Assert.That((await laneSecond).Succeeded, Is.True);
    Assert.That(Volatile.Read(ref laneMaxRunning), Is.EqualTo(1));
  }

  // General messages waiting at a busy receiver use up the sender's credit; the credit kept for lanes still
  // lets a lane request through.
  [Test]
  public async Task HandlerLanes_HaveCreditWhenGeneralTrafficUsesItUp()
  {
    IReadOnlyList<SocketHandlerLane> lanes = [new SocketHandlerLane("lane")];
    var eventGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      MaxConcurrentHandlers = 1,
      MaxQueuedHandlers = 8,
      HandlerLanes = lanes
    });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnEvent = async (_, _, cancellationToken) => await eventGate.Task.WaitAsync(OrderTimeout, cancellationToken),
      OnRequest = (_, _, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true })
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client", HandlerLanes = lanes });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    var events = Enumerable.Range(0, 20).Select(_ => client.SendEventAsync("general", "event", [1]).AsTask()).ToArray();
    await Task.Delay(200);
    Assert.That(events.Count(e => !e.IsCompleted), Is.GreaterThan(0), "the events should have used up the credit");

    var response = await client.RequestAsync("lane", "ping", [1], TimeSpan.FromSeconds(5));
    Assert.That(response.Succeeded, Is.True);

    eventGate.TrySetResult();
    await Task.WhenAll(events).WaitAsync(OrderTimeout);
  }

  [Test]
  public async Task HandlerLanes_DrainOnShutdown()
  {
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var finished = 0;
    var serverPeer = new TaskCompletionSource<SocketPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = new SocketListener(new SocketFabricOptions
    {
      NodeId = "server",
      HandlerLanes = [new SocketHandlerLane("lane", "slow", 1)]
    });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new DelegateSocketPeerHandler
    {
      OnConnect = (peer, _) =>
      {
        serverPeer.TrySetResult(peer);
        return ValueTask.CompletedTask;
      },
      OnRequest = async (_, _, cancellationToken) =>
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
        await Task.Delay(200);
        Volatile.Write(ref finished, 1);
        return new SocketResponse { Succeeded = true };
      }
    });

    var connector = new SocketConnector(new SocketFabricOptions { NodeId = "client" });
    await using var client = await connector.ConnectAsync(listener.EndPoint, new DelegateSocketPeerHandler());
    var request = client.RequestAsync("lane", "slow", [1], OrderTimeout).AsTask();
    await entered.Task.WaitAsync(OrderTimeout);

    // WaitForHandlersAsync covers lane workers too: it completes only once the lane handler is done.
    var closing = await serverPeer.Task;
    await closing.DisposeAsync();
    await closing.WaitForHandlersAsync().WaitAsync(OrderTimeout);
    Assert.That(Volatile.Read(ref finished), Is.EqualTo(1));
    await request.WaitAsync(OrderTimeout);
  }

  [Test]
  public void HandlerLanes_AreValidated()
  {
    // Rejected when the options are first used.
    foreach (var lanes in new IReadOnlyList<SocketHandlerLane>[]
    {
      [null],
      [new SocketHandlerLane(null)],
      [new SocketHandlerLane("")],
      [new SocketHandlerLane("test", "")],
      [new SocketHandlerLane("test", "a"), new SocketHandlerLane("test", "a")],
      [new SocketHandlerLane("test"), new SocketHandlerLane("test", MaxConcurrentHandlers: 4)]
    })
    {
      Assert.That(() => new SocketListener(new SocketFabricOptions { HandlerLanes = lanes }), Throws.ArgumentException);
    }

    // Limits are clamped like MaxConcurrentHandlers.
    var normalized = new SocketFabricOptions
    {
      HandlerLanes = [new SocketHandlerLane("low", MaxConcurrentHandlers: 0), new SocketHandlerLane("high", MaxConcurrentHandlers: 5000)]
    }.Normalize();
    Assert.That(normalized.HandlerLanes.Select(lane => lane.MaxConcurrentHandlers), Is.EqualTo(new[] { 1, 1024 }));

    // A service's lane and one of its operations' lanes may both exist; the operation's wins.
    var map = new SocketFabricOptions
    {
      HandlerLanes = [new SocketHandlerLane("test"), new SocketHandlerLane("test", "special")]
    }.Normalize().LaneMap;
    Assert.That(map.Find("test", "special"), Is.EqualTo(1));
    Assert.That(map.Find("test", "other"), Is.EqualTo(0));
    Assert.That(map.Find("other", "special"), Is.EqualTo(-1));
    Assert.That(map.Find(null, null), Is.EqualTo(-1));

    // The credit kept for lanes: their total limit, at most half the window, none for a window of one.
    var reserve = new SocketFabricOptions
    {
      HandlerLanes = [new SocketHandlerLane("a", MaxConcurrentHandlers: 2), new SocketHandlerLane("b", MaxConcurrentHandlers: 3)]
    }.Normalize().LaneMap;
    Assert.That(reserve.CreditReserve(100), Is.EqualTo(5));
    Assert.That(reserve.CreditReserve(8), Is.EqualTo(4));
    Assert.That(reserve.CreditReserve(1), Is.Zero);

    // Without lanes there is no map, so nothing is looked up per message.
    Assert.That(new SocketFabricOptions().Normalize().LaneMap, Is.Null);
  }

  static void InterlockedMax(ref int target, int value)
  {
    var current = Volatile.Read(ref target);
    while (value > current)
    {
      var seen = Interlocked.CompareExchange(ref target, value, current);
      if (seen == current)
        return;
      current = seen;
    }
  }
}
