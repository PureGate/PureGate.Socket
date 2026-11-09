using PureGate.Socket.Observability;
using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Framing;
using PureGate.Socket.Handling;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;

namespace PureGate.Socket.Transport;

public sealed class SocketConnector(SocketFabricOptions options = null)
{
  readonly SocketFabricOptions Options = (options ?? new SocketFabricOptions()).Normalize();

  // endpoint is an IPEndPoint or a DnsEndPoint. A host name is resolved on every call, so a reconnect
  // follows an address change, and each resolved address is tried in turn. HandshakeTimeout covers
  // resolution, connecting and the handshake together. A connect that HandshakeTimeout ends throws
  // SocketException(TimedOut), like any other unreachable node; only the caller's own token throws
  // OperationCanceledException.
  public async ValueTask<SocketPeer> ConnectAsync(
      EndPoint endpoint,
      ISocketPeerHandler handler,
      CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(endpoint);

    TcpClient client = null;
    Stream stream = null;

    try
    {
      using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      timeout.CancelAfter(Options.HandshakeTimeout);
      try
      {
        client = await ConnectTcpAsync(endpoint, Options.SocketBufferBytes, timeout.Token);
        stream = await Options.StreamFilter.WrapClientAsync(
            client.GetStream(),
            Options.NodeId,
            timeout.Token);
        var reader = SocketPeer.CreateReader(stream, Options);
        var (remoteNodeId, remoteWindow) = await RunClientHandshakeAsync(stream, reader, timeout.Token);
        return new SocketPeer(client, stream, reader, Options.NodeId, remoteNodeId, remoteWindow, Options, handler);
      }
      // HandshakeTimeout ended it, not the caller: an unreachable node, at any step.
      catch (OperationCanceledException) when (timeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
      {
        throw new SocketException((int)SocketError.TimedOut);
      }
    }
    catch (Exception ex)
    {
      SocketMetrics.HandshakeFailed("client", ex);
      stream?.Dispose();
      client?.Dispose();
      throw;
    }
  }

  // cancellationToken must end: when a connect gets no answer, the addresses are tried again until it does.
  static async ValueTask<TcpClient> ConnectTcpAsync(EndPoint endpoint, int socketBufferBytes, CancellationToken cancellationToken)
  {
    IPAddress[] addresses;
    int port;
    switch (endpoint)
    {
      case IPEndPoint ip:
        addresses = [ip.Address];
        port = ip.Port;
        break;
      case DnsEndPoint dns:
        addresses = await Dns.GetHostAddressesAsync(dns.Host, dns.AddressFamily, cancellationToken);
        port = dns.Port;
        if (addresses.Length == 0)
          throw new SocketException((int)SocketError.HostNotFound);
        break;
      default:
        throw new ArgumentException("Only IPEndPoint and DnsEndPoint are supported.", nameof(endpoint));
    }

    while (true)
    {
      SocketException lastFailure = null;
      var unanswered = false;
      foreach (var address in addresses)
      {
        // A socket whose connect failed cannot connect again, so each attempt takes a new one.
        var client = new TcpClient(address.AddressFamily) { NoDelay = true };
        SocketPeer.ApplySocketBuffers(client.Client, socketBufferBytes);
        var retriedHere = DisableSynRetransmissions(client.Client);
        try
        {
          await client.ConnectAsync(new IPEndPoint(address, port), cancellationToken);
          return client;
        }
        catch (SocketException ex)
        {
          client.Dispose();
          lastFailure = ex;
          unanswered |= retriedHere && ex.SocketErrorCode == SocketError.TimedOut;
        }
        catch
        {
          client.Dispose();
          throw;
        }
      }

      // Every address refused: no point trying again. One that got no answer may have lost its SYN, which
      // the OS no longer resends, so the round starts again from the first address.
      if (!unanswered)
        throw lastFailure ?? new SocketException((int)SocketError.HostNotFound);

      // An attempt with no answer normally takes the first timeout (about 1 s). Should a network driver report
      // the timeout sooner, this pause keeps the rounds from opening sockets non-stop.
      await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
    }
  }

  // SIO_TCP_INITIAL_RTO: keep the default first timeout (0xFFFF), no SYN retransmissions (0xFE).
  const int SioTcpInitialRto = unchecked((int)0x98000011);
  static readonly byte[] NoSynRetransmissions = [0xFF, 0xFF, 0xFE, 0x00];

  // Windows answers a refused connect by sending the SYN again, for about 2 s by default, before it fails.
  // With SYN retransmissions off, a refused connect fails at once, as on Linux; a connect that gets no
  // answer fails after the first timeout (about 1 s), and ConnectTcpAsync tries it again. So a silent
  // address costs about 1 s, not the whole HandshakeTimeout. Returns whether retransmissions are off: a
  // Windows version that rejects the setting keeps its own retries.
  static bool DisableSynRetransmissions(System.Net.Sockets.Socket socket)
  {
    if (!OperatingSystem.IsWindows())
      return false;
    try
    {
      socket.IOControl(SioTcpInitialRto, NoSynRetransmissions, null);
      return true;
    }
    catch (SocketException)
    {
      return false;
    }
  }

  async ValueTask<(string NodeId, int MessageWindow)> RunClientHandshakeAsync(Stream stream, SocketFrameReader reader, CancellationToken cancellationToken)
  {
    var writer = new SocketFrameWriter(
        stream,
        Options.MaxHandshakeFrameBytes);
    var hello = await Options.Authenticator.CreateHelloAsync(
        Options.NodeId,
        SocketFabricOptions.CurrentProtocolVersion,
        cancellationToken);
    await writer.WriteAsync(new SocketFrame
    {
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "hello",
      Payload = SocketHandshakeJson.Codec.Serialize(hello)
    }, cancellationToken);

    var challengeFrame = await reader.ReadAsync(cancellationToken);
    if (challengeFrame is null)
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed during handshake.");
    if (challengeFrame.Kind != SocketMessageKind.Event
        || !string.Equals(challengeFrame.Service, "socket", StringComparison.Ordinal)
        || !string.Equals(challengeFrame.Operation, "challenge", StringComparison.Ordinal))
    {
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake challenge.");
    }

    if (!challengeFrame.Ok)
      throw new SocketProtocolException(challengeFrame.ErrorCode ?? SocketProtocolError.Unauthorized, challengeFrame.ErrorMessage ?? "Handshake rejected.");

    var challenge = SocketHandshakeJson.Codec.Deserialize<SocketHandshakeChallenge>(challengeFrame.PayloadMemory)
        ?? throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake challenge.");
    if (challenge.Version != SocketFabricOptions.CurrentProtocolVersion)
      throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");

    var challengeAuth = await Options.Authenticator.AuthenticateChallengeAsync(hello, challenge, cancellationToken);
    if (!challengeAuth.Succeeded)
      throw new SocketProtocolException(SocketProtocolError.Unauthorized, challengeAuth.Error);
    await Options.StreamFilter.VerifyRemoteNodeAsync(stream, challengeAuth.RemoteNodeId, cancellationToken);

    var proof = await Options.Authenticator.CreateProofAsync(
        Options.NodeId,
        hello,
        challenge,
        cancellationToken);
    await writer.WriteAsync(new SocketFrame
    {
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "proof",
      Value = Options.MaxQueuedHandlers,
      Payload = SocketHandshakeJson.Codec.Serialize(proof)
    }, cancellationToken);

    var ready = await reader.ReadAsync(cancellationToken);
    if (ready is null)
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed during handshake.");
    if (ready.Kind != SocketMessageKind.Event
        || !string.Equals(ready.Service, "socket", StringComparison.Ordinal)
        || !string.Equals(ready.Operation, "ready", StringComparison.Ordinal))
    {
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake response.");
    }

    if (!ready.Ok)
      throw new SocketProtocolException(ready.ErrorCode ?? SocketProtocolError.Unauthorized, ready.ErrorMessage ?? "Handshake rejected.");

    return (challengeAuth.RemoteNodeId, SocketPeer.ReadHandshakeWindow(ready));
  }
}
