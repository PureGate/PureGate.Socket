using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using PureGate.Socket.Protocol;

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

  // Binds node IDs to certificates. When set, a connection is accepted only if the remote certificate
  // satisfies this check for the node ID the handshake authenticated, so a node holding a valid certificate
  // for one name cannot authenticate under another. On a listener this needs client certificates
  // (mutual TLS). CertificateNamesNode is the usual check.
  public Func<X509Certificate2, string, bool> RemoteNodeValidation { get; init; }

  // True when the certificate carries nodeId as a DNS subject alternative name (or as its common name
  // when it has no DNS names). Wildcards are not accepted.
  public static bool CertificateNamesNode(X509Certificate2 certificate, string nodeId)
  {
    if (certificate is null || string.IsNullOrWhiteSpace(nodeId))
      return false;

    try
    {
#if NET8_0_OR_GREATER
      return certificate.MatchesHostname(nodeId, allowWildcards: false, allowCommonName: true);
#else
      return CertificateHostnames.Matches(certificate, nodeId, allowWildcards: false, allowCommonName: true);
#endif
    }
    catch (ArgumentException)
    {
      return false;
    }
  }

  public static TlsSocketStreamFilter Create(
      X509Certificate2 serverCertificate,
      string targetHost,
      RemoteCertificateValidationCallback clientCertificateValidation = null,
      RemoteCertificateValidationCallback serverCertificateValidation = null,
      bool requireClientCertificate = false,
      X509CertificateCollection clientCertificates = null,
      bool bindNodeIdToCertificate = false)
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
        })
    {
      RemoteNodeValidation = bindNodeIdToCertificate ? CertificateNamesNode : null
    };
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

  public ValueTask VerifyRemoteNodeAsync(
      Stream stream,
      string remoteNodeId,
      CancellationToken cancellationToken)
  {
    var validation = RemoteNodeValidation;
    if (validation is null)
      return ValueTask.CompletedTask;

    if (stream is not SslStream { RemoteCertificate: { } remote })
      throw new SocketProtocolException(SocketProtocolError.Unauthorized, "Remote node presented no certificate.");

    var certificate = remote as X509Certificate2;
    var owned = certificate is null;
#if NET9_0_OR_GREATER
    certificate ??= X509CertificateLoader.LoadCertificate(remote.GetRawCertData());
#else
    certificate ??= new X509Certificate2(remote.GetRawCertData());
#endif
    try
    {
      if (!validation(certificate, remoteNodeId))
        throw new SocketProtocolException(SocketProtocolError.Unauthorized, "Remote certificate does not match the node ID.");
    }
    finally
    {
      if (owned)
        certificate.Dispose();
    }

    return ValueTask.CompletedTask;
  }
}
