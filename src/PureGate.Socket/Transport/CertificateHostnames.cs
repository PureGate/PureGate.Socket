using System.Formats.Asn1;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace PureGate.Socket.Transport;

// X509Certificate2.MatchesHostname is .NET 8 and later. This is a port of its .NET 8 implementation for .NET 6
// and 7, decoding the subject alternative names and the subject with System.Formats.Asn1, which those
// frameworks include. It is compiled on every framework so the tests can check it against MatchesHostname.
static class CertificateHostnames
{
  const string SubjectAltNameOid = "2.5.29.17";
  const string CommonNameOid = "2.5.4.3";

  public static bool Matches(X509Certificate2 certificate, string hostname, bool allowWildcards, bool allowCommonName)
  {
    ArgumentNullException.ThrowIfNull(certificate);
    ArgumentNullException.ThrowIfNull(hostname);

    if (!IPAddress.TryParse(hostname, out var ipAddress) && Uri.CheckHostName(hostname) != UriHostNameType.Dns)
      throw new ArgumentException("The value is not a valid DNS hostname or IP address.", nameof(hostname));

    X509Extension rawSan = null;
    foreach (var extension in certificate.Extensions)
    {
      if (extension.Oid?.Value != SubjectAltNameOid)
        continue;
      if (rawSan is not null)
        throw new CryptographicException("The certificate has more than one Subject Alternative Name extension.");
      rawSan = extension;
    }

    if (rawSan is not null)
    {
      var (dnsNames, ipAddresses) = DecodeSubjectAltNames(rawSan.RawData);
      var hadAny = false;

      if (ipAddress is not null)
      {
        foreach (var entry in ipAddresses)
        {
          if (entry.Equals(ipAddress))
            return true;
          hadAny = true;
        }
      }
      else
      {
        ReadOnlySpan<char> match = hostname;

        // Treat "something.example.org." as "something.example.org".
        if (hostname.EndsWith('.'))
        {
          match = match[..^1];
          if (match.IsEmpty)
            return false;
        }

        ReadOnlySpan<char> afterFirstDot = default;
        var firstDot = match.IndexOf('.');
        if (firstDot > 0)
          afterFirstDot = match[(firstDot + 1)..];

        foreach (var embedded in dnsNames)
        {
          hadAny = true;
          if (embedded.Length == 0)
            continue;

          ReadOnlySpan<char> embeddedSpan = embedded;
          if (embedded.EndsWith('.'))
            embeddedSpan = embeddedSpan[..^1];

          if (allowWildcards && embeddedSpan.StartsWith("*.") && embeddedSpan.Length > 2)
          {
            if (embeddedSpan[2..].Equals(afterFirstDot, StringComparison.OrdinalIgnoreCase))
              return true;
          }
          else if (embeddedSpan.Equals(match, StringComparison.OrdinalIgnoreCase))
          {
            return true;
          }
        }
      }

      if (hadAny)
        return false;
    }

    // The common name counts only when the subject has exactly one, outside any multi-valued RDN.
    return allowCommonName && hostname.Equals(SingleCommonName(certificate.SubjectName.RawData), StringComparison.OrdinalIgnoreCase);
  }

  // GeneralNames ::= SEQUENCE OF GeneralName (RFC 5280 4.2.1.6). Every entry is validated, as .NET does, so a
  // malformed extension fails the same way on every framework.
  static (List<string> DnsNames, List<IPAddress> IPAddresses) DecodeSubjectAltNames(byte[] rawData)
  {
    var dnsNames = new List<string>();
    var ipAddresses = new List<IPAddress>();
    try
    {
      var outer = new AsnReader(rawData, AsnEncodingRules.DER);
      var sequence = outer.ReadSequence();
      outer.ThrowIfNotEmpty();

      while (sequence.HasData)
      {
        var tag = sequence.PeekTag();
        if (tag.TagClass != TagClass.ContextSpecific)
          throw new CryptographicException("ASN1 corrupted data.");

        switch (tag.TagValue)
        {
          case 0: // otherName: SEQUENCE { type-id OBJECT IDENTIFIER, value [0] EXPLICIT ANY }
          {
            var otherName = sequence.ReadSequence(Context(0));
            otherName.ReadObjectIdentifier();
            var value = otherName.ReadSequence(Context(0, constructed: true));
            value.ReadEncodedValue();
            value.ThrowIfNotEmpty();
            otherName.ThrowIfNotEmpty();
            break;
          }
          case 1: // rfc822Name
          case 6: // uniformResourceIdentifier
            sequence.ReadCharacterString(UniversalTagNumber.IA5String, Context(tag.TagValue));
            break;
          case 2: // dNSName
            dnsNames.Add(sequence.ReadCharacterString(UniversalTagNumber.IA5String, Context(2)));
            break;
          case 3: // x400Address
            sequence.ReadEncodedValue();
            break;
          case 4: // directoryName: [4] EXPLICIT Name
          {
            var directoryName = sequence.ReadSequence(Context(4, constructed: true));
            directoryName.ReadEncodedValue();
            directoryName.ThrowIfNotEmpty();
            break;
          }
          case 5: // ediPartyName: SEQUENCE { nameAssigner [0] DirectoryString OPTIONAL, partyName [1] DirectoryString }
          {
            var ediPartyName = sequence.ReadSequence(Context(5));
            if (ediPartyName.HasData && ediPartyName.PeekTag().HasSameClassAndValue(Context(0)))
              ReadExplicitDirectoryString(ediPartyName, 0);
            ReadExplicitDirectoryString(ediPartyName, 1);
            ediPartyName.ThrowIfNotEmpty();
            break;
          }
          case 7: // iPAddress
          {
            var address = sequence.ReadOctetString(Context(7));
            if (address.Length is not (4 or 16))
              throw new CryptographicException("The Subject Alternative Name extension has an IP address of an unknown size.");
            ipAddresses.Add(new IPAddress(address));
            break;
          }
          case 8: // registeredID
            sequence.ReadObjectIdentifier(Context(8));
            break;
          default:
            throw new CryptographicException("ASN1 corrupted data.");
        }
      }
    }
    catch (AsnContentException e)
    {
      throw new CryptographicException("ASN1 corrupted data.", e);
    }
    return (dnsNames, ipAddresses);
  }

  // Name ::= SEQUENCE OF RelativeDistinguishedName; RelativeDistinguishedName ::= SET OF AttributeTypeAndValue.
  // Returns the common name, or null when there is none, more than one, or one inside a multi-valued RDN.
  static string SingleCommonName(byte[] subject)
  {
    try
    {
      var outer = new AsnReader(subject, AsnEncodingRules.DER);
      var name = outer.ReadSequence();
      outer.ThrowIfNotEmpty();

      ReadOnlyMemory<byte>? commonName = null;
      while (name.HasData)
      {
        // Windows does not sort multi-valued RDNs, so the sort order is not checked.
        var rdn = name.ReadSetOf(skipSortOrderValidation: true);
        var attributes = new List<(string Type, ReadOnlyMemory<byte> Value)>();
        while (rdn.HasData)
        {
          var attribute = rdn.ReadSequence();
          var type = attribute.ReadObjectIdentifier();
          var value = attribute.ReadEncodedValue();
          attribute.ThrowIfNotEmpty();
          attributes.Add((type, value));
        }
        if (attributes.Count == 0)
          throw new CryptographicException("ASN1 corrupted data.");

        if (attributes.Count > 1)
        {
          foreach (var (type, _) in attributes)
          {
            if (type == CommonNameOid)
              return null;
          }
        }
        else if (attributes[0].Type == CommonNameOid)
        {
          if (commonName is not null)
            return null;
          commonName = attributes[0].Value;
        }
      }

      return commonName is { } encoded ? DecodeText(encoded) : null;
    }
    catch (AsnContentException e)
    {
      throw new CryptographicException("ASN1 corrupted data.", e);
    }
  }

  // The textual value of an attribute, or null for a non-text type (as X500RelativeDistinguishedName does).
  static string DecodeText(ReadOnlyMemory<byte> encoded)
  {
    var reader = new AsnReader(encoded, AsnEncodingRules.DER);
    var tag = reader.PeekTag();
    if (tag.TagClass != TagClass.Universal)
      return null;

    switch ((UniversalTagNumber)tag.TagValue)
    {
      case UniversalTagNumber.BMPString:
      case UniversalTagNumber.UTF8String:
      case UniversalTagNumber.IA5String:
      case UniversalTagNumber.PrintableString:
      case UniversalTagNumber.NumericString:
      case UniversalTagNumber.T61String:
        return reader.ReadCharacterString((UniversalTagNumber)tag.TagValue);
      default:
        return null;
    }
  }

  // DirectoryString ::= CHOICE { teletexString, printableString, universalString, utf8String, bmpString }
  static void ReadExplicitDirectoryString(AsnReader reader, int tagNumber)
  {
    var wrapper = reader.ReadSequence(Context(tagNumber, constructed: true));
    var tag = wrapper.PeekTag();
    if (tag.TagClass != TagClass.Universal)
      throw new CryptographicException("ASN1 corrupted data.");

    switch ((UniversalTagNumber)tag.TagValue)
    {
      case UniversalTagNumber.T61String:
      case UniversalTagNumber.PrintableString:
      case UniversalTagNumber.UTF8String:
      case UniversalTagNumber.BMPString:
        wrapper.ReadCharacterString((UniversalTagNumber)tag.TagValue);
        break;
      case UniversalTagNumber.UniversalString:
        wrapper.ReadEncodedValue();
        break;
      default:
        throw new CryptographicException("ASN1 corrupted data.");
    }
    wrapper.ThrowIfNotEmpty();
  }

  static Asn1Tag Context(int tagNumber, bool constructed = false) => new(TagClass.ContextSpecific, tagNumber, constructed);
}
