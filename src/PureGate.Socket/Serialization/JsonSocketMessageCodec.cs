using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace PureGate.Socket.Serialization;

// System.Text.Json, writing straight into the connection's buffer and reading the received bytes in place.
//
// For AOT and trimming, pass a source-generated context: new JsonSocketMessageCodec(MyJsonContext.Default).
// Every message type must then be registered on that context ([JsonSerializable(typeof(MyMessage))]).
// Shared (and the options constructor without a resolver) use reflection-based serialization.
public sealed class JsonSocketMessageCodec : ISocketMessageCodec
{
  public static readonly JsonSocketMessageCodec Shared = new();

  [ThreadStatic]
  static Utf8JsonWriter CachedWriter;

  readonly JsonSerializerOptions Options;
  readonly JsonWriterOptions WriterOptions;

  // Web defaults (camelCase names, case-insensitive reads), reflection-based.
  public JsonSocketMessageCodec()
      : this(new JsonSerializerOptions(JsonSerializerDefaults.Web))
  {
  }

  public JsonSocketMessageCodec(JsonSerializerOptions options)
  {
    ArgumentNullException.ThrowIfNull(options);
    if (options.TypeInfoResolver is null)
    {
      // Copied so the caller's instance is not changed; reflection is the resolver of last resort.
      options = new JsonSerializerOptions(options) { TypeInfoResolver = CreateFallbackResolver() };
    }

    options.MakeReadOnly();
    Options = options;
    WriterOptions = new JsonWriterOptions
    {
      Encoder = options.Encoder,
      Indented = options.WriteIndented,
      MaxDepth = options.MaxDepth,
      SkipValidation = true
    };
  }

  // Source-generated metadata only: no reflection, safe for AOT and trimming.
  public JsonSocketMessageCodec(JsonSerializerContext context)
      : this(context?.Options ?? throw new ArgumentNullException(nameof(context)))
  {
  }

  public void Serialize<T>(T value, IBufferWriter<byte> output)
  {
    ArgumentNullException.ThrowIfNull(output);
    var writer = CachedWriter;
    CachedWriter = null;
    if (writer is null || !SameOptions(writer.Options))
      writer = new Utf8JsonWriter(output, WriterOptions);
    else
      writer.Reset(output);

    try
    {
      JsonSerializer.Serialize(writer, value, TypeInfo<T>());
      writer.Flush();
    }
    finally
    {
      writer.Reset();
      CachedWriter = writer;
    }
  }

  public T Deserialize<T>(ReadOnlySequence<byte> payload)
  {
    if (payload.IsSingleSegment)
      return JsonSerializer.Deserialize(payload.FirstSpan, TypeInfo<T>());

    var reader = new Utf8JsonReader(payload, new JsonReaderOptions
    {
      AllowTrailingCommas = Options.AllowTrailingCommas,
      CommentHandling = Options.ReadCommentHandling,
      MaxDepth = Options.MaxDepth
    });
    return JsonSerializer.Deserialize(ref reader, TypeInfo<T>());
  }

  // Reflection only when the runtime allows it. Under native AOT or with the System.Text.Json reflection feature switch
  // off, the resolver is empty, so a type without source-generated metadata fails with System.Text.Json's clear error.
  [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault.")]
  [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault.")]
  static IJsonTypeInfoResolver CreateFallbackResolver()
  {
    return JsonSerializer.IsReflectionEnabledByDefault
        ? new DefaultJsonTypeInfoResolver()
        : JsonTypeInfoResolver.Combine();
  }

  JsonTypeInfo<T> TypeInfo<T>()
  {
    return (JsonTypeInfo<T>)Options.GetTypeInfo(typeof(T));
  }

  bool SameOptions(JsonWriterOptions options)
  {
    return options.Indented == WriterOptions.Indented
        && options.MaxDepth == WriterOptions.MaxDepth
        && ReferenceEquals(options.Encoder, WriterOptions.Encoder)
        && options.SkipValidation;
  }
}
