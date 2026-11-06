using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace PureGate.Socket.Transport;

public sealed class TlsSocketStreamFilter : ISocketStreamFilter
{
  readonly Func<string, SslClientAuthenticationOptions> ClientOptionsFactory;
  readonly Func<string, SslServerAuthenticationOptions> ServerOptionsFactory;

  public TlsSocketStreamFilter(
      Func<string, SslClientAuthenticationOptions> clientOptionsFactory,
      Func<string, SslServerAuthenticationOptions> serverOptionsFactory)
  {
    ClientOptionsFactory = clientOptionsFactory ?? throw new ArgumentNullException(nameof(clientOptionsFactory));
    ServerOptionsFactory = serverOptionsFactory ?? throw new ArgumentNullException(nameof(serverOptionsFactory));
  }

  public static TlsSocketStreamFilter Create(
      X509Certificate2 serverCertificate,
      string targetHost,
      RemoteCertificateValidationCallback clientCertificateValidation = null,
      RemoteCertificateValidationCallback serverCertificateValidation = null,
      bool requireClientCertificate = false,
      X509CertificateCollection clientCertificates = null)
  {
    ArgumentNullException.ThrowIfNull(serverCertificate);
    targetHost = string.IsNullOrWhiteSpace(targetHost) ? serverCertificate.GetNameInfo(X509NameType.DnsName, false) : targetHost;
    if (string.IsNullOrWhiteSpace(targetHost))
      targetHost = "puregate.socket";

    return new TlsSocketStreamFilter(
        _ => new SslClientAuthenticationOptions
        {
          TargetHost = targetHost,
          ClientCertificates = clientCertificates,
          EnabledSslProtocols = SslProtocols.None,
          CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
          RemoteCertificateValidationCallback = serverCertificateValidation
        },
        _ => new SslServerAuthenticationOptions
        {
          ServerCertificate = serverCertificate,
          ClientCertificateRequired = requireClientCertificate,
          EnabledSslProtocols = SslProtocols.None,
          CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
          RemoteCertificateValidationCallback = clientCertificateValidation
        });
  }

  public async ValueTask<Stream> WrapClientAsync(
      Stream stream,
      string localNodeId,
      CancellationToken cancellationToken)
  {
    var tls = new SslStream(stream, leaveInnerStreamOpen: false);
    try
    {
      await tls.AuthenticateAsClientAsync(ClientOptionsFactory(localNodeId), cancellationToken);
      return tls;
    }
    catch
    {
      tls.Dispose();
      throw;
    }
  }

  public async ValueTask<Stream> WrapServerAsync(
      Stream stream,
      string localNodeId,
      CancellationToken cancellationToken)
  {
    var tls = new SslStream(stream, leaveInnerStreamOpen: false);
    try
    {
      await tls.AuthenticateAsServerAsync(ServerOptionsFactory(localNodeId), cancellationToken);
      return tls;
    }
    catch
    {
      tls.Dispose();
      throw;
    }
  }
}
