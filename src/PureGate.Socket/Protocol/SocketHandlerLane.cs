namespace PureGate.Socket.Protocol;

// A handler pool of its own, on every connection, for the requests and stream requests of one service
// (Operation null) or one operation of it. They run on the lane's workers, at most MaxConcurrentHandlers at
// once, from the lane's own queue, so they never wait behind the connection's general handlers, and the
// general handlers never wait behind them. Events always run on the general handlers.
//
// Lanes break a deadlock that bounded handler pools otherwise make possible: a handler that calls back the
// peer whose request it is serving, and waits for the answer. When both sides do that at once, every
// general handler on both sides can end up waiting for a callback that is queued behind them. Put the
// callback operations in a lane, and the callbacks always get a handler.
//
// Configure the same lanes on both ends of a connection. The receiver runs matching requests on the lane.
// The sender keeps part of its message credit for them, so that general messages queued at a busy
// receiver cannot use up the credit the lane's requests need to be sent at all.
//
// Matching uses the service and operation in each request; an operation's lane takes precedence over its
// service's. A batch request (RequestBatchAsync) runs as one request on the general handlers.
public sealed record SocketHandlerLane(string Service, string Operation = null, int MaxConcurrentHandlers = 16);
