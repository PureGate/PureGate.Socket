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
}
