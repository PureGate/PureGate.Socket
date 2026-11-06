using System.Text.Json;

namespace PureGate.Socket.Serialization;

public sealed class JsonSocketMessageCodec : ISocketMessageCodec
{
  public static readonly JsonSocketMessageCodec Shared = new();
  internal static readonly JsonSerializerOptions SharedOptions = new(JsonSerializerDefaults.Web);

  public byte[] Serialize<T>(T value)
  {
    return JsonSerializer.SerializeToUtf8Bytes(value, SharedOptions);
  }

  public T Deserialize<T>(ReadOnlySpan<byte> payload)
  {
    return JsonSerializer.Deserialize<T>(payload, SharedOptions);
  }
}
