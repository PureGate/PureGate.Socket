using System.Formats.Asn1;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Node IDs are bound to certificate names with X509Certificate2.MatchesHostname on .NET 8 and later, and with a
// port of it (CertificateHostnames) on .NET 6 and 7. Every case has a fixed expected answer, checked on every
// framework; on .NET 8 and later the port is also compared with MatchesHostname in every mode.
public sealed partial class SocketFabricTests
{
  const string CommonNameOid = "2.5.4.3";
  const string OrganizationOid = "2.5.4.10";

  sealed record HostnameCase(string Name, X509Certificate2 Certificate, string Hostname, string Expected);

  [Test]
  [Category("Tls")]
  public void CertificateNames_MatchLikeMatchesHostname()
  {
    var failures = new List<string>();
    foreach (var item in HostnameCases())
    {
      using (item.Certificate)
      {
        var port = Outcome(() => CertificateHostnames.Matches(item.Certificate, item.Hostname, allowWildcards: false, allowCommonName: true));
        if (port != item.Expected)
          failures.Add($"{item.Name} '{item.Hostname}': expected {item.Expected}, port gave {port}");

        // CertificateNamesNode treats a hostname that is not a DNS name or IP address as not matching.
        var expectedNode = item.Expected == "ArgumentException" ? "false" : item.Expected;
        var node = Outcome(() => TlsSocketStreamFilter.CertificateNamesNode(item.Certificate, item.Hostname));
        if (node != expectedNode)
          failures.Add($"{item.Name} '{item.Hostname}': expected {expectedNode} from CertificateNamesNode, got {node}");

#if NET8_0_OR_GREATER
        foreach (var wildcards in new[] { false, true })
        {
          foreach (var commonName in new[] { false, true })
          {
            var builtIn = Outcome(() => item.Certificate.MatchesHostname(item.Hostname, wildcards, commonName));
            var ported = Outcome(() => CertificateHostnames.Matches(item.Certificate, item.Hostname, wildcards, commonName));
            if (builtIn != ported)
              failures.Add($"{item.Name} '{item.Hostname}' (wildcards {wildcards}, common name {commonName}): MatchesHostname gave {builtIn}, port gave {ported}");
          }
        }
#endif
      }
    }

    Assert.That(failures, Is.Empty);
  }

  static IEnumerable<HostnameCase> HostnameCases()
  {
    X509Extension Dns(params string[] names)
    {
      var builder = new SubjectAlternativeNameBuilder();
      foreach (var name in names)
        builder.AddDnsName(name);
      return builder.Build();
    }

    var cnNodeB = new[] { new[] { (CommonNameOid, "node-b") } };

    foreach (var hostname in new[] { "node-a", "NODE-A", "node-a.", "node-b", "", ".", "bad host", "node-a..", "10.0.0.1" })
      yield return Case("dns name", Subject(cnNodeB), hostname, hostname switch
      {
        "node-a" or "NODE-A" or "node-a." => "true",
        "" or "." or "bad host" or "node-a.." => "ArgumentException",
        _ => "false"
      }, Dns("node-a"));

    // The common name is not used when the certificate has DNS names.
    yield return Case("dns name hides the common name", Subject(cnNodeB), "node-b", "false", Dns("node-a"));

    yield return Case("trailing dot in the certificate", Subject(cnNodeB), "node-a", "true", Dns("node-a."));
    yield return Case("several dns names", Subject(cnNodeB), "node-c", "true", Dns("node-a", "node-c"));

    foreach (var hostname in new[] { "a.example.org", "example.org", "*.example.org", "a.b.example.org" })
      yield return Case("wildcard", Subject(cnNodeB), hostname, hostname.StartsWith('*') ? "ArgumentException" : "false", Dns("*.example.org"));

    // Entries of other types neither match nor stop the fallback to the common name.
    var others = new SubjectAlternativeNameBuilder();
    others.AddEmailAddress("node-b@example.org");
    others.AddUri(new Uri("https://node-b.example.org/"));
    others.AddUserPrincipalName("node-b@example.org");
    yield return Case("other entry types", Subject(cnNodeB), "node-b", "true", others.Build());

    yield return Case("no alternative names", Subject(cnNodeB), "node-b", "true");
    yield return Case("no alternative names", Subject(cnNodeB), "NODE-B", "true");
    yield return Case("no alternative names", Subject(cnNodeB), "node-c", "false");
    yield return Case("no common name", Subject(new[] { new[] { (OrganizationOid, "node-b") } }), "node-b", "false");
    yield return Case("two common names", Subject(new[] { new[] { (CommonNameOid, "node-b") }, new[] { (CommonNameOid, "node-b") } }), "node-b", "false");
    yield return Case("multi-valued common name", Subject(new[] { new[] { (CommonNameOid, "node-b"), (OrganizationOid, "org") } }), "node-b", "false");
    yield return Case("organization next to the common name", Subject(new[] { new[] { (OrganizationOid, "org") }, new[] { (CommonNameOid, "node-b") } }), "node-b", "true");
    yield return Case("BMPString common name", Subject(new[] { new[] { (CommonNameOid, "node-b") } }, UniversalTagNumber.BMPString), "node-b", "true");
    yield return Case("UTF8String common name", Subject(new[] { new[] { (CommonNameOid, "node-b") } }, UniversalTagNumber.UTF8String), "node-b", "true");
    yield return Case("non-text common name", OctetStringCommonName(), "node-b", "false");

    // IP addresses match iPAddress entries only, and fall back to the common name when there are none.
    var addresses = new SubjectAlternativeNameBuilder();
    addresses.AddIpAddress(IPAddress.Parse("10.0.0.1"));
    addresses.AddIpAddress(IPAddress.IPv6Loopback);
    addresses.AddDnsName("node-a");
    var cnAddress = new[] { new[] { (CommonNameOid, "10.0.0.9") } };
    foreach (var (hostname, expected) in new[] { ("10.0.0.1", "true"), ("::1", "true"), ("10.0.0.2", "false"), ("10.0.0.9", "false"), ("node-a", "true") })
      yield return Case("ip addresses", Subject(cnAddress), hostname, expected, addresses.Build());
    yield return Case("ip without ip entries", Subject(cnAddress), "10.0.0.9", "true", Dns("node-a"));
    yield return Case("numeric common name", Subject(new[] { new[] { (CommonNameOid, "12345") } }, UniversalTagNumber.NumericString), "12345", "true");

    // Raw extensions: an empty DNS name, registeredID and directoryName entries, and malformed encodings.
    yield return Case("empty dns name", Subject(cnNodeB), "node-b", "false", RawAltNames(w => w.WriteCharacterString(UniversalTagNumber.IA5String, "", Context(2))));
    yield return Case("registeredID and directoryName", Subject(cnNodeB), "node-a", "true", RawAltNames(w =>
    {
      w.WriteObjectIdentifier("1.2.3.4", Context(8));
      w.PushSequence(Context(4, constructed: true));
      w.PushSequence();
      w.PopSequence();
      w.PopSequence(Context(4, constructed: true));
      w.WriteCharacterString(UniversalTagNumber.IA5String, "node-a", Context(2));
    }));
    yield return Case("ip of five bytes", Subject(cnNodeB), "node-b", "CryptographicException", RawAltNames(w => w.WriteOctetString(new byte[5], Context(7))));
    yield return Case("universal tag entry", Subject(cnNodeB), "node-b", "CryptographicException", RawAltNames(w => w.WriteCharacterString(UniversalTagNumber.IA5String, "node-b")));
    yield return Case("trailing data", Subject(cnNodeB), "node-b", "CryptographicException", new X509Extension("2.5.29.17", new byte[] { 0x30, 0x00, 0x00 }, false));
  }

  static HostnameCase Case(string name, X500DistinguishedName subject, string hostname, string expected, params X509Extension[] extensions)
  {
    using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
    foreach (var extension in extensions)
      request.CertificateExtensions.Add(extension);
    var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    return new HostnameCase(name, certificate, hostname, expected);
  }

  static X500DistinguishedName Subject((string Oid, string Value)[][] rdns, UniversalTagNumber type = UniversalTagNumber.PrintableString)
  {
    var writer = new AsnWriter(AsnEncodingRules.DER);
    writer.PushSequence();
    foreach (var rdn in rdns)
    {
      writer.PushSetOf();
      foreach (var (oid, value) in rdn)
      {
        writer.PushSequence();
        writer.WriteObjectIdentifier(oid);
        writer.WriteCharacterString(type, value);
        writer.PopSequence();
      }
      writer.PopSetOf();
    }
    writer.PopSequence();
    return new X500DistinguishedName(writer.Encode());
  }

  static X500DistinguishedName OctetStringCommonName()
  {
    var writer = new AsnWriter(AsnEncodingRules.DER);
    writer.PushSequence();
    writer.PushSetOf();
    writer.PushSequence();
    writer.WriteObjectIdentifier(CommonNameOid);
    writer.WriteOctetString("node-b"u8);
    writer.PopSequence();
    writer.PopSetOf();
    writer.PopSequence();
    return new X500DistinguishedName(writer.Encode());
  }

  static X509Extension RawAltNames(Action<AsnWriter> entries)
  {
    var writer = new AsnWriter(AsnEncodingRules.DER);
    writer.PushSequence();
    entries(writer);
    writer.PopSequence();
    return new X509Extension("2.5.29.17", writer.Encode(), false);
  }

  static Asn1Tag Context(int number, bool constructed = false) => new(TagClass.ContextSpecific, number, constructed);

  static string Outcome(Func<bool> match)
  {
    try
    {
      return match() ? "true" : "false";
    }
    catch (ArgumentException)
    {
      return "ArgumentException";
    }
    catch (CryptographicException)
    {
      return "CryptographicException";
    }
  }
}
