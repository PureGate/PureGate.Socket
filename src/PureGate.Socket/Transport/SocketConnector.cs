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
  // resolution, connecting and the handshake together.
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
      client = await ConnectTcpAsync(endpoint, timeout.Token);
      stream = await Options.StreamFilter.WrapClientAsync(
          client.GetStream(),
          Options.NodeId,
          timeout.Token);
      var reader = SocketPeer.CreateReader(stream, Options);
      var (remoteNodeId, remoteWindow) = await RunClientHandshakeAsync(stream, reader, timeout.Token);
      return new SocketPeer(client, stream, reader, Options.NodeId, remoteNodeId, remoteWindow, Options, handler);
    }
    catch
    {
      stream?.Dispose();
      client?.Dispose();
      throw;
    }
  }

  static async ValueTask<TcpClient> ConnectTcpAsync(EndPoint endpoint, CancellationToken cancellationToken)
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

    SocketException lastFailure = null;
    foreach (var address in addresses)
    {
      var client = new TcpClient(address.AddressFamily) { NoDelay = true };
      try
      {
        await client.ConnectAsync(new IPEndPoint(address, port), cancellationToken);
        return client;
      }
      catch (SocketException ex)
      {
        client.Dispose();
        lastFailure = ex;
      }
      catch
      {
        client.Dispose();
        throw;
      }
    }

    throw lastFailure;
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
      Payload = JsonSocketMessageCodec.Shared.Serialize(hello)
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

    var challenge = JsonSocketMessageCodec.Shared.Deserialize<SocketHandshakeChallenge>(challengeFrame.PayloadMemory.Span);
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
      Payload = JsonSocketMessageCodec.Shared.Serialize(proof)
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
