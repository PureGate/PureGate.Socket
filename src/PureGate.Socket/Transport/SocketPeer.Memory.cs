using PureGate.Socket.Framing;
using PureGate.Socket.Observability;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

// The connection's memory budget (SocketMemoryBudget, sized by MaxConnectionMemoryBytes) and the work it
// guards: reserving room for what the remote sends, refusing what does not fit, and decompressing
// payloads where they are used instead of on the read loop.
public sealed partial class SocketPeer
{
  const string OverBudgetMessage = "Connection memory budget exceeded.";

  readonly SocketMemoryBudget MemoryBudget;

  // Frees a frame's payload and gives its reservation back.
  void ReleaseFrame(SocketFrame frame)
  {
    frame.ReleasePayload();
    MemoryBudget.Release(frame.ReservedBytes);
    frame.ReservedBytes = 0;
  }

  // Read loop: reserves room for a handler-bound frame until its handler finishes. A reassembled frame
  // already holds its reassembly buffer.
  bool TryReserveFrame(SocketFrame frame)
  {
    if (frame.ReservedBytes > 0 || frame.PayloadLength == 0)
      return true;
    if (!MemoryBudget.TryReserve(frame.PayloadLength))
      return false;

    frame.ReservedBytes = frame.PayloadLength;
    return true;
  }

  // Refuses a message that does not fit the budget, in the way its kind allows. creditTaken is true when
  // the message never reached the handler queue, so its sender's credit still has to come back.
  void RejectOverBudget(SocketMessageKind kind, long messageId, bool creditTaken)
  {
    SocketMetrics.MemoryRejected(kind);
    switch (kind)
    {
      case SocketMessageKind.Request:
        if (creditTaken)
          ReturnDroppedMessageCredit();
        SendControl(new SocketFrame
        {
          MessageId = messageId,
          Kind = SocketMessageKind.Response,
          Ok = false,
          ErrorCode = SocketProtocolError.Overloaded,
          ErrorMessage = OverBudgetMessage
        });
        break;
      case SocketMessageKind.StreamRequest:
        if (creditTaken)
          ReturnDroppedMessageCredit();
        SendControl(new SocketFrame
        {
          MessageId = messageId,
          Kind = SocketMessageKind.StreamStart,
          Ok = false,
          ErrorCode = SocketProtocolError.Overloaded,
          ErrorMessage = OverBudgetMessage
        });
        break;
      case SocketMessageKind.Response:
        // The response to one of our own requests: that request fails here instead.
        DispatchResponse(new SocketFrame
        {
          MessageId = messageId,
          Kind = SocketMessageKind.Response,
          Ok = false,
          ErrorCode = SocketProtocolError.Overloaded,
          ErrorMessage = OverBudgetMessage
        });
        break;
      case SocketMessageKind.StreamItem:
        FailIncomingStream(messageId, new SocketProtocolException(SocketProtocolError.Overloaded, OverBudgetMessage));
        break;
      default:
        // An event has no reply, so closing the connection is the only way to tell its sender.
        throw new SocketProtocolException(SocketProtocolError.Overloaded, OverBudgetMessage);
    }
  }

  // Handler worker: decompresses a handler-bound frame before its handler runs. Returns false when the
  // payload did not fit the budget (the message has then been refused).
  bool TryDecompressHandlerFrame(SocketFrame frame)
  {
    if (!SocketCompression.TryDecompress(
            frame.PayloadMemory.Span,
            Options.MaxDecompressedPayloadSizeBytes,
            MemoryBudget,
            pooled: false,
            out var payload,
            out var reserved))
    {
      RejectOverBudget(frame.Kind, frame.MessageId, creditTaken: false);
      return false;
    }

    ReleaseFrame(frame);
    frame.SetPayload(payload);
    frame.ReservedBytes = reserved;
    frame.Flags &= ~SocketFrameFlags.PayloadCompressed;
    return true;
  }

  // Caller's thread: decompresses a response once it has been handed over. The payload now belongs to
  // the caller, so it is bounded by MaxDecompressedPayloadSizeBytes but not by the connection budget.
  SocketRequestResult DecompressResult(SocketRequestResult result)
  {
    try
    {
      SocketCompression.TryDecompress(
          result.Payload.Span,
          Options.MaxDecompressedPayloadSizeBytes,
          budget: null,
          pooled: false,
          out var payload,
          out _);
      return new SocketRequestResult
      {
        Succeeded = result.Succeeded,
        Payload = payload.Memory,
        ErrorCode = result.ErrorCode,
        ErrorMessage = result.ErrorMessage
      };
    }
    catch (SocketProtocolException ex)
    {
      // A peer that sends undecodable data is broken; the connection cannot be trusted any further.
      _ = CloseAsync(ex).AsTask();
      return new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = ex.ErrorCode,
        ErrorMessage = ex.Message
      };
    }
    finally
    {
      // The compressed bytes are no longer needed.
      result.ReleasePooled();
    }
  }
}
