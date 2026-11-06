using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Security;

// Mutual challenge-response authentication with HMAC-SHA256.
//
// With one shared secret, every node that knows it can claim any node ID. With per-node secrets
// (a lookup from the remote node ID to the secret this node shares with it), a node can only
// authenticate as an ID whose secret it holds: C, holding only the C-B secret, cannot pose as A to B.
// The lookup must be symmetric: A's lookup("B") and B's lookup("A") return the same bytes.
public sealed class HmacSocketAuthenticator : ISocketAuthenticator
{
  readonly Func<string, byte[]> SecretFor;
  readonly bool HasSecret;

  // Secret used to sign for unknown nodes, so an unknown ID fails exactly like a wrong signature and
  // the handshake does not reveal which node IDs exist.
  readonly byte[] DecoySecret = RandomNumberGenerator.GetBytes(32);

  public HmacSocketAuthenticator(string sharedSecret)
  {
    var secret = Encoding.UTF8.GetBytes(sharedSecret ?? string.Empty);
    HasSecret = secret.Length > 0;
    SecretFor = _ => secret;
  }

  // secretForRemoteNode returns the secret shared with the given remote node, or null when that node
  // is unknown (the handshake then fails).
  public HmacSocketAuthenticator(Func<string, byte[]> secretForRemoteNode)
  {
    SecretFor = secretForRemoteNode ?? throw new ArgumentNullException(nameof(secretForRemoteNode));
    HasSecret = true;
  }

  public ValueTask<SocketHandshakeHello> CreateHelloAsync(
      string localNodeId,
      int protocolVersion,
      CancellationToken cancellationToken)
  {
    EnsureSecret();
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
    var secret = Lookup(remoteHello.NodeId) ?? DecoySecret;
    return ValueTask.FromResult(new SocketHandshakeChallenge(
        remoteHello.Version,
        localNodeId ?? string.Empty,
        nonce,
        Sign(secret, "challenge", remoteHello.Version, localNodeId, remoteHello.NodeId, remoteHello.Nonce, nonce)));
  }

  public ValueTask<SocketAuthenticationResult> AuthenticateChallengeAsync(
      SocketHandshakeHello localHello,
      SocketHandshakeChallenge remoteChallenge,
      CancellationToken cancellationToken)
  {
    if (!HasSecret)
      return ValueTask.FromResult(SocketAuthenticationResult.Failed("Shared secret is required."));

    if (!TryValidateHello(localHello, out var helloError))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed(helloError));
    if (!TryValidateChallenge(remoteChallenge, out var challengeError))
      return ValueTask.FromResult(SocketAuthenticationResult.Failed(challengeError));

    var secret = Lookup(remoteChallenge.NodeId);
    if (secret is null)
      return ValueTask.FromResult(SocketAuthenticationResult.Failed("Remote node is not known."));

    var expected = Sign(
        secret,
        "challenge",
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
    var secret = Lookup(remoteChallenge.NodeId)
        ?? throw new InvalidOperationException("Remote node is not known.");
    return ValueTask.FromResult(new SocketHandshakeProof(
        localHello.Version,
        localNodeId ?? string.Empty,
        Sign(
            secret,
            "proof",
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
    if (!HasSecret)
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

    var secret = Lookup(remoteHello.NodeId) ?? DecoySecret;
    var expected = Sign(
        secret,
        "proof",
        remoteHello.Version,
        remoteHello.NodeId,
        localChallenge.NodeId,
        remoteHello.Nonce,
        localChallenge.Nonce);
    var ok = !ReferenceEquals(secret, DecoySecret) && SignatureEquals(expected, remoteProof.Signature);
    return ValueTask.FromResult(ok
        ? SocketAuthenticationResult.Success(remoteProof.NodeId)
        : SocketAuthenticationResult.Failed("Invalid proof signature."));
  }

  byte[] Lookup(string remoteNodeId)
  {
    var secret = SecretFor(remoteNodeId);
    return secret is { Length: > 0 } ? secret : null;
  }

  void EnsureSecret()
  {
    if (!HasSecret)
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

  // Signs a domain tag plus each field length-prefixed, so no choice of node IDs or nonces can make two
  // different handshakes produce the same signed bytes.
  static string Sign(
      byte[] secret,
      string kind,
      int version,
      string signerNodeId,
      string peerNodeId,
      string clientNonce,
      string serverNonce)
  {
    using var data = new MemoryStream();
    Append(data, "PureGate.Socket.v1");
    Append(data, kind);
    Append(data, version.ToString(System.Globalization.CultureInfo.InvariantCulture));
    Append(data, signerNodeId);
    Append(data, peerNodeId);
    Append(data, clientNonce);
    Append(data, serverNonce);
    return Convert.ToBase64String(HMACSHA256.HashData(secret, data.GetBuffer().AsSpan(0, (int)data.Length)));
  }

  static void Append(MemoryStream data, string value)
  {
    var bytes = Encoding.UTF8.GetBytes(value ?? string.Empty);
    Span<byte> length = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
    data.Write(length);
    data.Write(bytes);
  }

  static bool SignatureEquals(string expected, string actual)
  {
    return CryptographicOperations.FixedTimeEquals(
        Encoding.UTF8.GetBytes(expected ?? string.Empty),
        Encoding.UTF8.GetBytes(actual ?? string.Empty));
  }
}
