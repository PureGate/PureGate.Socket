using System.Security.Cryptography;
using System.Text;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Security;

public sealed class HmacSocketAuthenticator(string sharedSecret) : ISocketAuthenticator
{
  readonly byte[] Secret = Encoding.UTF8.GetBytes(sharedSecret ?? string.Empty);

  public ValueTask<SocketHandshakeHello> CreateHelloAsync(
      string localNodeId,
      int protocolVersion,
      CancellationToken cancellationToken)
  {
    if (Secret.Length == 0)
      throw new InvalidOperationException("Shared secret is required.");

    var nonce = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
    return ValueTask.FromResult(new SocketHandshakeHello(
        protocolVersion,
        localNodeId ?? string.Empty,
        nonce));
  }

  public ValueTask<SocketHandshakeChallenge> CreateChallengeAsync(
      string localNodeId,
      SocketHandshakeHello remoteHello,
      CancellationToken cancellationToken)
  {
    EnsureSecret();
    ValidateHello(remoteHello);
    var nonce = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
    return ValueTask.FromResult(new SocketHandshakeChallenge(
        remoteHello.Version,
        localNodeId ?? string.Empty,
        nonce,
        SignChallenge(remoteHello.Version, localNodeId, remoteHello.NodeId, remoteHello.Nonce, nonce)));
  }

  public ValueTask<SocketAuthenticationResult> AuthenticateChallengeAsync(
      SocketHandshakeHello localHello,
      SocketHandshakeChallenge remoteChallenge,
      CancellationToken cancellationToken)
  {
    if (Secret.Length == 0)
      return ValueTask.FromResult(SocketAuthenticationResult.Failed("Shared secret is required."));

    if (!TryValidateHello(localHello, out var helloError))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed(helloError));
    if (!TryValidateChallenge(remoteChallenge, out var challengeError))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed(challengeError));

    var expected = SignChallenge(
        localHello.Version,
        remoteChallenge.NodeId,
        localHello.NodeId,
        localHello.Nonce,
        remoteChallenge.Nonce);
    var ok = SignatureEquals(expected, remoteChallenge.Signature);
    return ValueTask.FromResult(ok
        ? SocketAuthenticationResult.Success(remoteChallenge.NodeId)
        : SocketAuthenticationResult.Failed("Invalid challenge signature."));
  }

  public ValueTask<SocketHandshakeProof> CreateProofAsync(
      string localNodeId,
      SocketHandshakeHello localHello,
      SocketHandshakeChallenge remoteChallenge,
      CancellationToken cancellationToken)
  {
    EnsureSecret();
    ValidateHello(localHello);
    ValidateChallenge(remoteChallenge);
    return ValueTask.FromResult(new SocketHandshakeProof(
        localHello.Version,
        localNodeId ?? string.Empty,
        SignProof(
            localHello.Version,
            localNodeId,
            remoteChallenge.NodeId,
            localHello.Nonce,
            remoteChallenge.Nonce)));
  }

  public ValueTask<SocketAuthenticationResult> AuthenticateProofAsync(
      SocketHandshakeHello remoteHello,
      SocketHandshakeChallenge localChallenge,
      SocketHandshakeProof remoteProof,
      CancellationToken cancellationToken)
  {
    if (Secret.Length == 0)
      return ValueTask.FromResult(SocketAuthenticationResult.Failed("Shared secret is required."));

    if (!TryValidateHello(remoteHello, out var helloError))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed(helloError));
    if (!TryValidateChallenge(localChallenge, out var challengeError))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed(challengeError));
    if (remoteProof is null
        || string.IsNullOrWhiteSpace(remoteProof.NodeId)
        || string.IsNullOrWhiteSpace(remoteProof.Signature)
        || remoteProof.Version != remoteHello.Version
        || !string.Equals(remoteProof.NodeId, remoteHello.NodeId, StringComparison.Ordinal))
    {
      return ValueTask.FromResult(SocketAuthenticationResult.Failed("Invalid proof."));
    }

    var expected = SignProof(
        remoteHello.Version,
        remoteHello.NodeId,
        localChallenge.NodeId,
        remoteHello.Nonce,
        localChallenge.Nonce);
    var ok = SignatureEquals(expected, remoteProof.Signature);
    return ValueTask.FromResult(ok
        ? SocketAuthenticationResult.Success(remoteProof.NodeId)
        : SocketAuthenticationResult.Failed("Invalid proof signature."));
  }

  void EnsureSecret()
  {
    if (Secret.Length == 0)
      throw new InvalidOperationException("Shared secret is required.");
  }

  static void ValidateHello(SocketHandshakeHello hello)
  {
    if (!TryValidateHello(hello, out var error))
      throw new InvalidOperationException(error);
  }

  static bool TryValidateHello(SocketHandshakeHello hello, out string error)
  {
    error = null;
    if (hello is null
        || hello.Version < 1
        || string.IsNullOrWhiteSpace(hello.NodeId)
        || string.IsNullOrWhiteSpace(hello.Nonce))
    {
      error = "Invalid hello.";
      return false;
    }

    return true;
  }

  static void ValidateChallenge(SocketHandshakeChallenge challenge)
  {
    if (!TryValidateChallenge(challenge, out var error))
      throw new InvalidOperationException(error);
  }

  static bool TryValidateChallenge(SocketHandshakeChallenge challenge, out string error)
  {
    error = null;
    if (challenge is null
        || challenge.Version < 1
        || string.IsNullOrWhiteSpace(challenge.NodeId)
        || string.IsNullOrWhiteSpace(challenge.Nonce)
        || string.IsNullOrWhiteSpace(challenge.Signature))
    {
      error = "Invalid challenge.";
      return false;
    }

    return true;
  }

  string SignChallenge(
      int version,
      string serverNodeId,
      string clientNodeId,
      string clientNonce,
      string serverNonce)
  {
    return Sign("challenge", version, serverNodeId, clientNodeId, clientNonce, serverNonce);
  }

  string SignProof(
      int version,
      string clientNodeId,
      string serverNodeId,
      string clientNonce,
      string serverNonce)
  {
    return Sign("proof", version, clientNodeId, serverNodeId, clientNonce, serverNonce);
  }

  string Sign(
      string kind,
      int version,
      string signerNodeId,
      string peerNodeId,
      string clientNonce,
      string serverNonce)
  {
    var data = Encoding.UTF8.GetBytes(string.Concat(
        "PureGate.Socket.v1\n",
        kind ?? string.Empty,
        "\n",
        version.ToString(System.Globalization.CultureInfo.InvariantCulture),
        "\n",
        signerNodeId ?? string.Empty,
        "\n",
        peerNodeId ?? string.Empty,
        "\n",
        clientNonce ?? string.Empty,
        "\n",
        serverNonce ?? string.Empty));
    using var hmac = new HMACSHA256(Secret);
    return Convert.ToBase64String(hmac.ComputeHash(data));
  }

  static bool SignatureEquals(string expected, string actual)
  {
    return CryptographicOperations.FixedTimeEquals(
        Encoding.UTF8.GetBytes(expected ?? string.Empty),
        Encoding.UTF8.GetBytes(actual ?? string.Empty));
  }
}
