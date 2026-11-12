using System;
using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Reads cluster node identity from internode mTLS certificates.</summary>
internal static class MtlsCertificateIdentity
{
    /// <summary>The server authentication extended key usage OID.</summary>
    internal const string ServerAuthOid = "1.3.6.1.5.5.7.3.1";

    /// <summary>The client authentication extended key usage OID.</summary>
    internal const string ClientAuthOid = "1.3.6.1.5.5.7.3.2";

    private static readonly UniversalTagNumber[] CommonNameStringTypes =
        [UniversalTagNumber.UTF8String, UniversalTagNumber.PrintableString, UniversalTagNumber.T61String, UniversalTagNumber.BMPString];

    /// <summary>Requires the chain to allow both TLS roles: every certificate with an extended key usage extension must list server and client authentication.</summary>
    /// <remarks>Every node accepts and opens internode connections, so one certificate serves both roles. A certificate without the extension is valid for any purpose and stays accepted.</remarks>
    /// <param name="policy">The chain policy to restrict.</param>
    internal static void RequireNodeRoles(X509ChainPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _ = policy.ApplicationPolicy.Add(new Oid(ServerAuthOid));
        _ = policy.ApplicationPolicy.Add(new Oid(ClientAuthOid));
    }

    /// <summary>Returns whether the certificate common name matches the expected cluster node identifier.</summary>
    /// <param name="certificate">ServerPeer or node certificate.</param>
    /// <param name="expectedNodeId">Configured cluster node identifier.</param>
    /// <returns><see langword="true" /> when identities match using ordinal comparison.</returns>
    internal static bool MatchesNodeId(X509Certificate2 certificate, string expectedNodeId)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedNodeId);

        return TryGetNodeId(certificate, out var nodeId) && string.Equals(nodeId, expectedNodeId, StringComparison.Ordinal);
    }

    /// <summary>Reads the cluster <see cref="ServerPeer.NodeId" /> from the certificate common name.</summary>
    /// <param name="certificate">ServerPeer or node certificate.</param>
    /// <param name="nodeId">Parsed node identifier when present.</param>
    /// <returns><see langword="true" /> when the subject contains exactly one non-empty common name.</returns>
    internal static bool TryGetNodeId(X509Certificate2 certificate, out string nodeId)
    {
        ArgumentNullException.ThrowIfNull(certificate);

        nodeId = string.Empty;
        string? commonName = null;
        try
        {
            var subject = new AsnReader(certificate.SubjectName.RawData, AsnEncodingRules.DER);
            var names = subject.ReadSequence();
            subject.ThrowIfNotEmpty();
            while (names.HasData)
            {
                var attributes = names.ReadSetOf();
                while (attributes.HasData)
                {
                    var attribute = attributes.ReadSequence();
                    var type = attribute.ReadObjectIdentifier();
                    var value = attribute.ReadEncodedValue();
                    attribute.ThrowIfNotEmpty();
                    if (!string.Equals(type, "2.5.4.3", StringComparison.Ordinal))
                        continue;
                    if (commonName != null)
                        return false;

                    commonName = ReadCommonName(value);
                    if (string.IsNullOrWhiteSpace(commonName))
                        return false;
                }
            }
        }
        catch (AsnContentException)
        {
            return false;
        }

        if (commonName == null)
            return false;

        nodeId = commonName;
        return true;
    }

    private static string? ReadCommonName(ReadOnlyMemory<byte> encoded)
    {
        var reader = new AsnReader(encoded, AsnEncodingRules.DER);
        var tag = reader.PeekTag();
        foreach (var type in CommonNameStringTypes)
        {
            if (tag.HasSameClassAndValue(new Asn1Tag(type)))
                return reader.ReadCharacterString(type);
        }

        return null;
    }
}
