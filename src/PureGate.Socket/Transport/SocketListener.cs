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
  TcpListener Listener;
  CancellationTokenSource Lifetime;
  Task AcceptTask;

  public IPEndPoint EndPoint => (IPEndPoint)Listener?.LocalEndpoint;

  public ValueTask StartAsync(
      IPEndPoint endpoint,
      ISocketPeerHandler handler,
      CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(endpoint);
    cancellationToken.ThrowIfCancellationRequested();
    lock (Sync)
    {
      if (Listener is not null)
        throw new InvalidOperationException("Socket listener is already started.");

      Lifetime = new CancellationTokenSource();
      Listener = new TcpListener(endpoint);
      Listener.Start();
      AcceptTask = Task.Run(() => AcceptLoopAsync(handler ?? EmptySocketPeerHandler.Shared));
    }

    return ValueTask.CompletedTask;
  }

  async Task AcceptLoopAsync(ISocketPeerHandler handler)
  {
    while (!Lifetime.IsCancellationRequested)
    {
      TcpClient client = null;
      try
      {
        client = await Listener.AcceptTcpClientAsync(Lifetime.Token);
        client.NoDelay = true;
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
        if (Lifetime.IsCancellationRequested)
          break;

        ObserveListenerAcceptFailed(ex);
      }
    }
  }

  async Task AcceptPeerAsync(TcpClient client, ISocketPeerHandler handler)
  {
    Stream stream = null;
    try
    {
      using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Lifetime.Token);
      timeout.CancelAfter(Options.HandshakeTimeout);
      stream = await Options.StreamFilter.WrapServerAsync(
          client.GetStream(),
          Options.NodeId,
          timeout.Token);
      var reader = SocketPeer.CreateReader(stream, Options);
      var remoteNodeId = await RunServerHandshakeAsync(stream, reader, timeout.Token);
      var peer = new SocketPeer(client, stream, reader, Options.NodeId, remoteNodeId, Options, handler);
      lock (Sync)
        Peers.Add(peer);
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

  void ObservePeerAcceptFailed(TcpClient client, Exception exception)
  {
    IPEndPoint remoteEndPoint = null;
    try
    {
      remoteEndPoint = client.Client.RemoteEndPoint as IPEndPoint;
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

  async ValueTask<string> RunServerHandshakeAsync(Stream stream, SocketFrameReader reader, CancellationToken cancellationToken)
  {
    var writer = new SocketFrameWriter(
        stream,
        Options.MaxFrameSizeBytes);
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
      if (frame.Version != Options.ProtocolVersion)
        throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");

      var remoteHello = JsonSocketMessageCodec.Shared.Deserialize<SocketHandshakeHello>(frame.PayloadMemory.Span);
      if (remoteHello.Version != Options.ProtocolVersion)
        throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");

      var challenge = await Options.Authenticator.CreateChallengeAsync(
          Options.NodeId,
          remoteHello,
          cancellationToken);
      await writer.WriteAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        Kind = SocketMessageKind.Event,
        Service = "socket",
        Operation = "challenge",
        Ok = true,
        Payload = JsonSocketMessageCodec.Shared.Serialize(challenge)
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

      var proof = JsonSocketMessageCodec.Shared.Deserialize<SocketHandshakeProof>(proofFrame.PayloadMemory.Span);
      var auth = await Options.Authenticator.AuthenticateProofAsync(
          remoteHello,
          challenge,
          proof,
          cancellationToken);
      if (!auth.Succeeded)
        throw new SocketProtocolException(SocketProtocolError.Unauthorized, auth.Error);

      await writer.WriteAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        Kind = SocketMessageKind.Event,
        Service = "socket",
        Operation = "ready",
        Ok = true
      }, cancellationToken);
      return auth.RemoteNodeId;
    }
    catch (SocketProtocolException ex)
    {
      await writer.WriteAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
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

  public async ValueTask DisposeAsync()
  {
    Lifetime?.Cancel();
    Listener?.Stop();
    if (AcceptTask is not null)
    {
      try
      {
        await AcceptTask;
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

    Lifetime?.Dispose();
  }
}
