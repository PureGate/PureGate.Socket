namespace PureGate.Socket.Protocol;

public sealed record SocketHandshakeHello(
    int Version,
    string NodeId,
    string Nonce);

public sealed record SocketHandshakeChallenge(
    int Version,
    string NodeId,
    string Nonce,
    string Signature);

public sealed record SocketHandshakeProof(
    int Version,
    string NodeId,
    string Signature);
