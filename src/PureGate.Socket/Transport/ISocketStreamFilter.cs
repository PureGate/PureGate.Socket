namespace PureGate.Socket.Transport;

public interface ISocketStreamFilter
{
  ValueTask<Stream> WrapClientAsync(
      Stream stream,
      string localNodeId,
      CancellationToken cancellationToken);

  ValueTask<Stream> WrapServerAsync(
      Stream stream,
      string localNodeId,
      CancellationToken cancellationToken);

  // Called once the handshake has authenticated the remote node ID, before the connection is used. A
  // filter that knows the remote identity at the transport level (a TLS client certificate, for
  // instance) throws here when that identity does not match remoteNodeId. stream is the one this filter
  // returned.
  ValueTask VerifyRemoteNodeAsync(
      Stream stream,
      string remoteNodeId,
      CancellationToken cancellationToken)
  {
    return ValueTask.CompletedTask;
  }
}
