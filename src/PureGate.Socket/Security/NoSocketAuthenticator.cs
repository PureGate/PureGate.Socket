using PureGate.Socket.Protocol;

namespace PureGate.Socket.Security;

public sealed class NoSocketAuthenticator : ISocketAuthenticator
{
  public static readonly NoSocketAuthenticator Shared = new();

  public ValueTask<SocketHandshakeHello> CreateHelloAsync(
      string localNodeId,
      int protocolVersion,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(new SocketHandshakeHello(
        protocolVersion,
        localNodeId ?? string.Empty,
        Guid.NewGuid().ToString("N")));
  }

  public ValueTask<SocketHandshakeChallenge> CreateChallengeAsync(
      string localNodeId,
      SocketHandshakeHello remoteHello,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(new SocketHandshakeChallenge(
        remoteHello?.Version ?? SocketFabricOptions.CurrentProtocolVersion,
        localNodeId ?? string.Empty,
        Guid.NewGuid().ToString("N"),
        string.Empty));
  }

  public ValueTask<SocketAuthenticationResult> AuthenticateChallengeAsync(
      SocketHandshakeHello localHello,
      SocketHandshakeChallenge remoteChallenge,
      CancellationToken cancellationToken)
  {
    if (remoteChallenge is null || string.IsNullOrWhiteSpace(remoteChallenge.NodeId))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed("Remote node id is required."));

    return ValueTask.FromResult(SocketAuthenticationResult.Success(remoteChallenge.NodeId));
  }

  public ValueTask<SocketHandshakeProof> CreateProofAsync(
      string localNodeId,
      SocketHandshakeHello localHello,
      SocketHandshakeChallenge remoteChallenge,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(new SocketHandshakeProof(
        localHello?.Version ?? SocketFabricOptions.CurrentProtocolVersion,
        localNodeId ?? string.Empty,
        string.Empty));
  }

  public ValueTask<SocketAuthenticationResult> AuthenticateProofAsync(
      SocketHandshakeHello remoteHello,
      SocketHandshakeChallenge localChallenge,
      SocketHandshakeProof remoteProof,
      CancellationToken cancellationToken)
  {
    if (remoteProof is null || string.IsNullOrWhiteSpace(remoteProof.NodeId))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed("Remote node id is required."));

    return ValueTask.FromResult(SocketAuthenticationResult.Success(remoteProof.NodeId));
  }
}
