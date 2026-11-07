using PureGate.Socket.Observability;
using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Framing;
using PureGate.Socket.Handling;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;

namespace PureGate.Socket.Transport;

public sealed class SocketListener(SocketFabricOptions options = null) : IAsyncDisposable
{
  readonly SocketFabricOptions Options = (options ?? new SocketFabricOptions()).Normalize();
  readonly List<SocketPeer> Peers = [];
  readonly List<Task> PeerTasks = [];
  readonly Lock Sync = new();
  System.Net.Sockets.Socket Listener;
  IPEndPoint BoundEndPoint;
  CancellationTokenSource Lifetime;
  Task AcceptTask;
  int PendingHandshakes;
  bool Disposed;

  // The bound endpoint, with the port the system chose when port 0 was asked for. Null before Bind; still
  // reported after DisposeAsync.
  public IPEndPoint EndPoint => BoundEndPoint;

  // Reserves the endpoint without accepting anything: until StartAsync, connection attempts are refused at
  // once, so a caller learns the port (and can tell others about it) before it serves.
  public void Bind(IPEndPoint endpoint)
  {
    ArgumentNullException.ThrowIfNull(endpoint);
    lock (Sync)
    {
      if (Disposed)
        throw new ObjectDisposedException(nameof(SocketListener));
      if (Listener is not null)
        throw new InvalidOperationException("Socket listener is already bound.");

      var listener = new System.Net.Sockets.Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
      try
      {
        // Accepted connections inherit these, from before they are established.
        SocketPeer.ApplySocketBuffers(listener, Options.SocketBufferBytes);
        listener.Bind(endpoint);
        BoundEndPoint = (IPEndPoint)listener.LocalEndPoint;
      }
      catch
      {
        listener.Dispose();
        throw;
      }

      Listener = listener;
    }
  }

  // Starts accepting connections on the endpoint given to Bind.
  public ValueTask StartAsync(
      ISocketPeerHandler handler,
      CancellationToken cancellationToken = default)
  {
    cancellationToken.ThrowIfCancellationRequested();
    lock (Sync)
    {
      if (Disposed)
        throw new ObjectDisposedException(nameof(SocketListener));
      if (Listener is null)
        throw new InvalidOperationException("Socket listener is not bound; call Bind first.");
      if (Lifetime is not null)
        throw new InvalidOperationException("Socket listener is already started.");

      Listener.Listen((int)SocketOptionName.MaxConnections); // the system's largest backlog, as TcpListener uses
      Lifetime = new CancellationTokenSource();
      AcceptTask = Task.Run(() => AcceptLoopAsync(handler ?? EmptySocketPeerHandler.Shared));
    }

    return ValueTask.CompletedTask;
  }

  // Binds and starts accepting in one step.
  public ValueTask StartAsync(
      IPEndPoint endpoint,
      ISocketPeerHandler handler,
      CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(endpoint);
    cancellationToken.ThrowIfCancellationRequested();
    Bind(endpoint);
    return StartAsync(handler, cancellationToken);
  }

  // Started by StartAsync once Listener and Lifetime are set.
  async Task AcceptLoopAsync(ISocketPeerHandler handler)
  {
    var listener = Listener;
    var lifetime = Lifetime;
    while (!lifetime.IsCancellationRequested)
    {
      System.Net.Sockets.Socket client = null;
      try
      {
        client = await listener.AcceptAsync(lifetime.Token);
        client.NoDelay = true;
        if (!TryAdmit(client))
        {
          client = null;
          continue;
        }

        var peerTask = Task.Run(() => AcceptPeerAsync(client, handler));
        lock (Sync)
          PeerTasks.Add(peerTask);
        _ = peerTask.ContinueWith(task =>
        {
          lock (Sync)
            PeerTasks.Remove(task);
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
      }
      catch (OperationCanceledException)
      {
        client?.Dispose();
        break;
      }
      catch (Exception ex)
      {
        client?.Dispose();
        if (lifetime.IsCancellationRequested)
          break;

        ObserveListenerAcceptFailed(ex);
      }
    }
  }

  // Closes a new connection at once when the listener is at its limits, so unauthenticated peers
  // cannot hold an unbounded number of handshakes (or connections) open.
  bool TryAdmit(System.Net.Sockets.Socket client)
  {
    int established;
    lock (Sync)
      established = Peers.Count;

    var pending = Volatile.Read(ref PendingHandshakes);
    string reason = null;
    if (pending >= Options.MaxPendingHandshakes)
      reason = "Too many handshakes are in progress.";
    else if ((long)established + pending >= Options.MaxConnections)
      reason = "The listener is at its connection limit.";

    if (reason is null)
    {
      Interlocked.Increment(ref PendingHandshakes);
      return true;
    }

    ObservePeerAcceptFailed(client, new SocketProtocolException(SocketProtocolError.Overloaded, reason));
    client.Dispose();
    return false;
  }

  async Task AcceptPeerAsync(System.Net.Sockets.Socket client, ISocketPeerHandler handler)
  {
    Stream stream = null;
    var admitted = true;
    try
    {
      using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Lifetime.Token);
      timeout.CancelAfter(Options.HandshakeTimeout);
      stream = await Options.StreamFilter.WrapServerAsync(
          new NetworkStream(client, ownsSocket: true),
          Options.NodeId,
          timeout.Token);
      var reader = SocketPeer.CreateReader(stream, Options);
      var (remoteNodeId, remoteWindow) = await RunServerHandshakeAsync(stream, reader, timeout.Token);
      var peer = new SocketPeer(client, stream, reader, Options.NodeId, remoteNodeId, remoteWindow, Options, handler);
      lock (Sync)
        Peers.Add(peer);
      Interlocked.Decrement(ref PendingHandshakes);
      admitted = false;
      _ = Task.Run(async () =>
      {
        await peer.Completion;
        lock (Sync)
          Peers.Remove(peer);
      });
    }
    catch (Exception ex)
    {
      ObservePeerAcceptFailed(client, ex);
      stream?.Dispose();
      client.Dispose();
    }
    finally
    {
      if (admitted)
        Interlocked.Decrement(ref PendingHandshakes);
    }
  }

  void ObserveListenerAcceptFailed(Exception exception)
  {
    try
    {
      Options.Observer.ListenerAcceptFailed(EndPoint, exception);
    }
    catch
    {
    }
  }

  void ObservePeerAcceptFailed(System.Net.Sockets.Socket client, Exception exception)
  {
    SocketMetrics.HandshakeFailed("server", exception);
    IPEndPoint remoteEndPoint = null;
    try
    {
      remoteEndPoint = client.RemoteEndPoint as IPEndPoint;
    }
    catch
    {
    }

    try
    {
      Options.Observer.PeerAcceptFailed(remoteEndPoint, exception);
    }
    catch
    {
    }
  }

  async ValueTask<(string NodeId, int MessageWindow)> RunServerHandshakeAsync(Stream stream, SocketFrameReader reader, CancellationToken cancellationToken)
  {
    var writer = new SocketFrameWriter(
        stream,
        Options.MaxHandshakeFrameBytes);
    var frame = await reader.ReadAsync(cancellationToken);
    if (frame is null)
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed during handshake.");
    if (frame.Kind != SocketMessageKind.Event
        || !string.Equals(frame.Service, "socket", StringComparison.Ordinal)
        || !string.Equals(frame.Operation, "hello", StringComparison.Ordinal))
    {
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake request.");
    }

    try
    {
      var remoteHello = SocketHandshakeJson.Codec.Deserialize<SocketHandshakeHello>(frame.PayloadMemory)
        ?? throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake hello.");
      if (remoteHello.Version != SocketFabricOptions.CurrentProtocolVersion)
        throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");

      var challenge = await Options.Authenticator.CreateChallengeAsync(
          Options.NodeId,
          remoteHello,
          cancellationToken);
      await writer.WriteAsync(new SocketFrame
      {
        Kind = SocketMessageKind.Event,
        Service = "socket",
        Operation = "challenge",
        Ok = true,
        Payload = SocketHandshakeJson.Codec.Serialize(challenge)
      }, cancellationToken);

      var proofFrame = await reader.ReadAsync(cancellationToken);
      if (proofFrame is null)
        throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed during handshake.");
      if (proofFrame.Kind != SocketMessageKind.Event
          || !string.Equals(proofFrame.Service, "socket", StringComparison.Ordinal)
          || !string.Equals(proofFrame.Operation, "proof", StringComparison.Ordinal))
      {
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake proof.");
      }

      var proof = SocketHandshakeJson.Codec.Deserialize<SocketHandshakeProof>(proofFrame.PayloadMemory)
        ?? throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake proof.");
      var remoteWindow = SocketPeer.ReadHandshakeWindow(proofFrame);
      var auth = await Options.Authenticator.AuthenticateProofAsync(
          remoteHello,
          challenge,
          proof,
          cancellationToken);
      if (!auth.Succeeded)
        throw new SocketProtocolException(SocketProtocolError.Unauthorized, auth.Error);
      await Options.StreamFilter.VerifyRemoteNodeAsync(stream, auth.RemoteNodeId, cancellationToken);

      await writer.WriteAsync(new SocketFrame
      {
        Kind = SocketMessageKind.Event,
        Service = "socket",
        Operation = "ready",
        Ok = true,
        Value = Options.MaxQueuedHandlers
      }, cancellationToken);
      return (auth.RemoteNodeId, remoteWindow);
    }
    catch (SocketProtocolException ex)
    {
      await writer.WriteAsync(new SocketFrame
      {
        Kind = SocketMessageKind.Event,
        Service = "socket",
        Operation = "ready",
        Ok = false,
        ErrorCode = ex.ErrorCode,
        ErrorMessage = ex.Message
      }, cancellationToken);
      throw;
    }
  }

  // Works in every state (never bound, bound, started) and releases the port. Only the first call does the
  // work; later calls, including concurrent ones, return at once.
  public async ValueTask DisposeAsync()
  {
    System.Net.Sockets.Socket listener;
    CancellationTokenSource lifetime;
    Task acceptTask;
    lock (Sync)
    {
      if (Disposed)
        return;
      Disposed = true;
      listener = Listener;
      lifetime = Lifetime;
      acceptTask = AcceptTask;
    }

    lifetime?.Cancel();
    listener?.Dispose();
    if (acceptTask is not null)
    {
      try
      {
        await acceptTask;
      }
      catch
      {
      }
    }

    SocketPeer[] peers;
    Task[] peerTasks;
    lock (Sync)
    {
      peers = [.. Peers];
      peerTasks = [.. PeerTasks];
    }
    foreach (var peer in peers)
      await peer.DisposeAsync();

    if (peerTasks.Length > 0)
    {
      try
      {
        await Task.WhenAll(peerTasks);
      }
      catch
      {
      }
    }

    lifetime?.Dispose();
  }
}
