// Stand-ins for newer APIs the tests use, so the same tests run on every framework the library targets. Each is
// compiled only where the real API is missing.

#if !NET9_0_OR_GREATER
global using Lock = object;

namespace System.Security.Cryptography.X509Certificates
{
  // X509CertificateLoader is .NET 9 and later.
  internal static class X509CertificateLoader
  {
    public static X509Certificate2 LoadPkcs12(byte[] data, string password, X509KeyStorageFlags keyStorageFlags) =>
      new(data, password, keyStorageFlags);
  }
}
#endif

namespace PureGate.Socket.Test
{
#if !NET8_0_OR_GREATER
  // On .NET 6 and 7 the tests use NUnit 4, whose Assert.ThrowsAsync waits for the delegate and returns the
  // exception itself; NUnit 5 returns a task of it. This lets `await Assert.ThrowsAsync<T>(...)` compile on both.
  static class NUnitCompatibility
  {
    public static System.Runtime.CompilerServices.TaskAwaiter<T> GetAwaiter<T>(this T exception) where T : Exception =>
      Task.FromResult(exception).GetAwaiter();
  }
#endif

#if !NET7_0_OR_GREATER
  static class StreamCompatibility
  {
    // Stream.ReadAtLeastAsync is .NET 7 and later.
    public static async ValueTask<int> ReadAtLeastAsync(
        this Stream stream,
        Memory<byte> buffer,
        int minimumBytes,
        bool throwOnEndOfStream = true,
        CancellationToken cancellationToken = default)
    {
      var total = 0;
      while (total < minimumBytes)
      {
        var read = await stream.ReadAsync(buffer[total..], cancellationToken);
        if (read == 0)
        {
          if (throwOnEndOfStream)
            throw new EndOfStreamException();
          break;
        }
        total += read;
      }
      return total;
    }
  }

  static class SpanCompatibility
  {
    // MemoryExtensions.IndexOfAnyExcept is .NET 7 and later.
    public static int IndexOfAnyExcept(this ReadOnlySpan<byte> span, byte value)
    {
      for (var i = 0; i < span.Length; i++)
      {
        if (span[i] != value)
          return i;
      }
      return -1;
    }
  }
#endif
}
