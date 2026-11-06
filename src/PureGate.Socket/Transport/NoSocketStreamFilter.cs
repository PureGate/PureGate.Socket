namespace PureGate.Socket.Transport;

public sealed class NoSocketStreamFilter : ISocketStreamFilter
{
  public static readonly NoSocketStreamFilter Shared = new();

  public ValueTask<Stream> WrapClientAsync(
      Stream stream,
      string localNodeId,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(stream);
  }

  public ValueTask<Stream> WrapServerAsync(
      Stream stream,
      string localNodeId,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(stream);
  }
}
