namespace PureGate.Socket.Protocol;

// How events received on one connection are handed to ISocketPeerHandler.HandleEventAsync.
// Requests and stream requests always run concurrently, whatever this is set to.
public enum SocketEventOrdering
{
  // One event at a time, in the order the peer sent them. Safe for "invalidate, then update" patterns.
  Sequential = 0,

  // In order within each service and operation; different operations run concurrently.
  PerOperation = 1,

  // As many at a time as MaxConcurrentHandlers allows, in no particular order.
  Concurrent = 2
}
