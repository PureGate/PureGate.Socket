using PureGate.Socket.Protocol;

namespace PureGate.Socket.Security;

public interface ISocketAuthenticator
{
  ValueTask<SocketHandshakeHello> CreateHelloAsync(
      string localNodeId,
      int protocolVersion,
      CancellationToken cancellationToken);

  ValueTask<SocketHandshakeChallenge> CreateChallengeAsync(
      string localNodeId,
      SocketHandshakeHello remoteHello,
      CancellationToken cancellationToken);

  ValueTask<SocketAuthenticationResult> AuthenticateChallengeAsync(
      SocketHandshakeHello localHello,
      SocketHandshakeChallenge remoteChallenge,
      CancellationToken cancellationToken);

  ValueTask<SocketHandshakeProof> CreateProofAsync(
      string localNodeId,
      SocketHandshakeHello localHello,
      SocketHandshakeChallenge remoteChallenge,
      CancellationToken cancellationToken);

  ValueTask<SocketAuthenticationResult> AuthenticateProofAsync(
      SocketHandshakeHello remoteHello,
      SocketHandshakeChallenge localChallenge,
      SocketHandshakeProof remoteProof,
      CancellationToken cancellationToken);
}
