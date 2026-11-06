using System.Text.Json;
using System.Text.Json.Serialization;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Serialization;

// The handshake messages are JSON with web naming (see docs/PROTOCOL.md). Source-generated, so the
// handshake needs no reflection and works under AOT and trimming whatever codec the application uses.
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(SocketHandshakeHello))]
[JsonSerializable(typeof(SocketHandshakeChallenge))]
[JsonSerializable(typeof(SocketHandshakeProof))]
partial class SocketHandshakeJson : JsonSerializerContext
{
  // Created on first use: initializers in the generated half of this class may run after ours.
  static JsonSocketMessageCodec CodecValue;

  public static JsonSocketMessageCodec Codec => CodecValue ??= new JsonSocketMessageCodec(Default);
}
