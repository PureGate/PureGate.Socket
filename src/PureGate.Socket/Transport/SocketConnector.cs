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

  public async ValueTask<SocketPeer> ConnectAsync(
      IPEndPoint endpoint,
      ISocketPeerHandler handler,
      CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(endpoint);

    var client = new TcpClient(endpoint.AddressFamily)
    {
      NoDelay = true
    };
    Stream stream = null;

    try
    {
      using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      timeout.CancelAfter(Options.HandshakeTimeout);
      await client.ConnectAsync(endpoint, timeout.Token);
      stream = await Options.StreamFilter.WrapClientAsync(
          client.GetStream(),
          Options.NodeId,
          timeout.Token);
      var remoteNodeId = await RunClientHandshakeAsync(stream, timeout.Token);
      return new SocketPeer(client, stream, Options.NodeId, remoteNodeId, Options, handler);
    }
    catch
    {
      stream?.Dispose();
      client.Dispose();
      throw;
    }
  }

  async ValueTask<string> RunClientHandshakeAsync(Stream stream, CancellationToken cancellationToken)
  {
    var reader = new SocketFrameReader(stream, Options.MaxFrameSizeBytes);
    var writer = new SocketFrameWriter(
        stream,
        Options.MaxFrameSizeBytes);
    var hello = await Options.Authenticator.CreateHelloAsync(
        Options.NodeId,
        Options.ProtocolVersion,
        cancellationToken);
    await writer.WriteAsync(new SocketFrame
    {
      Version = Options.ProtocolVersion,
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "hello",
      Payload = JsonSocketMessageCodec.Shared.Serialize(hello)
    }, cancellationToken);

    var challengeFrame = await reader.ReadAsync(cancellationToken);
    if (challengeFrame is null)
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed during handshake.");
    if (challengeFrame.Version != Options.ProtocolVersion)
      throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");
    if (challengeFrame.Kind != SocketMessageKind.Event
        || !string.Equals(challengeFrame.Service, "socket", StringComparison.Ordinal)
        || !string.Equals(challengeFrame.Operation, "challenge", StringComparison.Ordinal))
    {
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake challenge.");
    }

    if (!challengeFrame.Ok)
      throw new SocketProtocolException(challengeFrame.ErrorCode ?? SocketProtocolError.Unauthorized, challengeFrame.ErrorMessage ?? "Handshake rejected.");

    var challenge = JsonSocketMessageCodec.Shared.Deserialize<SocketHandshakeChallenge>(challengeFrame.PayloadMemory.Span);
    if (challenge.Version != Options.ProtocolVersion)
      throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");

    var challengeAuth = await Options.Authenticator.AuthenticateChallengeAsync(hello, challenge, cancellationToken);
    if (!challengeAuth.Succeeded)
      throw new SocketProtocolException(SocketProtocolError.Unauthorized, challengeAuth.Error);

    var proof = await Options.Authenticator.CreateProofAsync(
        Options.NodeId,
        hello,
        challenge,
        cancellationToken);
    await writer.WriteAsync(new SocketFrame
    {
      Version = Options.ProtocolVersion,
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "proof",
      Payload = JsonSocketMessageCodec.Shared.Serialize(proof)
    }, cancellationToken);

    var ready = await reader.ReadAsync(cancellationToken);
    if (ready is null)
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed during handshake.");
    if (ready.Version != Options.ProtocolVersion)
      throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");
    if (ready.Kind != SocketMessageKind.Event
        || !string.Equals(ready.Service, "socket", StringComparison.Ordinal)
        || !string.Equals(ready.Operation, "ready", StringComparison.Ordinal))
    {
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid handshake response.");
    }

    if (!ready.Ok)
      throw new SocketProtocolException(ready.ErrorCode ?? SocketProtocolError.Unauthorized, ready.ErrorMessage ?? "Handshake rejected.");

    return challengeAuth.RemoteNodeId;
  }
}
