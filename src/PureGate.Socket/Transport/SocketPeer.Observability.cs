using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  void ObservePeerConnected()
  {
    try
    {
      Options.Observer.PeerConnected(this);
    }
    catch
    {
    }
  }

  void ObservePeerDisconnected(Exception exception)
  {
    try
    {
      Options.Observer.PeerDisconnected(this, exception);
    }
    catch
    {
    }
  }

  void ObserveFrameReceived(SocketFrame frame)
  {
    try
    {
      Options.Observer.FrameReceived(this, frame.Kind, frame.PayloadLength);
    }
    catch
    {
    }
  }

  // payloadBytes is the payload actually sent, which for responses and stream chunks is passed separately
  // from the frame (the frame's own payload is then empty).
  void ObserveFrameSent(SocketFrame frame, int payloadBytes)
  {
    ObserveFrameSent(frame.Kind, payloadBytes);
  }

  void ObserveFrameSent(SocketMessageKind kind, int payloadBytes)
  {
    try
    {
      Options.Observer.FrameSent(this, kind, payloadBytes);
    }
    catch
    {
    }
  }

  void ObserveRequestStarted(SocketFrame frame)
  {
    try
    {
      Options.Observer.RequestStarted(this, frame.Service, frame.Operation);
    }
    catch
    {
    }
  }

  void ObserveHandlerFailed(string service, string operation, Exception exception)
  {
    try
    {
      Options.Observer.HandlerFailed(this, service, operation, exception);
    }
    catch
    {
    }
  }

  void ObserveRequestCompleted(SocketFrame frame, SocketResponse response)
  {
    try
    {
      Options.Observer.RequestCompleted(
          this,
          frame.Service,
          frame.Operation,
          response?.Succeeded == true,
          response?.ErrorCode);
    }
    catch
    {
    }
  }
}
