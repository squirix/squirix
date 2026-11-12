using System;
using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Verifies the certificate subject used for cluster node identity.</summary>
[Immutable]
public sealed class MtlsCertificateIdentityTests : ServerUnitTestBase
{
    /// <summary>Verifies only an explicit, unambiguous common name identifies a node.</summary>
    /// <param name="subject">The certificate subject.</param>
    [Test]
    [Arguments("OU=node-b")]
    [Arguments("O=node-b")]
    [Arguments("E=node-b@example.test")]
    [Arguments("CN=node-b, CN=node-c")]
    [Arguments("CN=\" \"")]
    public async Task AmbiguousOrMissingCommonNameIsRefused(string subject)
    {
        using var certificate = CreateCertificate(subject);

        _ = await Assert.That(MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId)).IsFalse();
        _ = await Assert.That(nodeId).IsEqualTo(string.Empty);
        _ = await Assert.That(MtlsCertificateIdentity.MatchesNodeId(certificate, "node-b")).IsFalse();
    }

    /// <summary>Verifies an explicit common name is preserved and matched ordinally.</summary>
    /// <param name="subject">The certificate subject.</param>
    [Test]
    [Arguments("CN=node-b")]
    [Arguments("OU=other-node, CN=node-b, O=cluster")]
    public async Task ExplicitCommonNameIdentifiesNode(string subject)
    {
        using var certificate = CreateCertificate(subject);

        _ = await Assert.That(MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId)).IsTrue();
        _ = await Assert.That(nodeId).IsEqualTo("node-b");
        _ = await Assert.That(MtlsCertificateIdentity.MatchesNodeId(certificate, "node-b")).IsTrue();
        _ = await Assert.That(MtlsCertificateIdentity.MatchesNodeId(certificate, "NODE-B")).IsFalse();
    }

    /// <summary>Verifies supported directory string encodings retain the same node identity.</summary>
    /// <param name="encoding">The common-name string encoding.</param>
    [Test]
    [Arguments(UniversalTagNumber.UTF8String)]
    [Arguments(UniversalTagNumber.PrintableString)]
    [Arguments(UniversalTagNumber.T61String)]
    [Arguments(UniversalTagNumber.BMPString)]
    public async Task EncodedCommonNameIdentifiesNode(UniversalTagNumber encoding)
    {
        var writer = new AsnWriter(AsnEncodingRules.DER);
        using (writer.PushSequence())
        using (writer.PushSetOf())
            WriteAttribute(writer, "2.5.4.3", encoding, "node-b");

        using var certificate = CreateCertificate(new X500DistinguishedName(writer.Encode()));
        _ = await Assert.That(MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId)).IsTrue();
        _ = await Assert.That(nodeId).IsEqualTo("node-b");
    }

    /// <summary>Verifies multi-valued relative names accept one common name and reject two.</summary>
    /// <param name="duplicateCommonName">Whether the second attribute is another common name.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MultiValuedNameRequiresSingleCommonName(bool duplicateCommonName)
    {
        var writer = new AsnWriter(AsnEncodingRules.DER);
        using (writer.PushSequence())
        using (writer.PushSetOf())
        {
            WriteAttribute(writer, "2.5.4.3", UniversalTagNumber.UTF8String, "node-b");
            WriteAttribute(writer, duplicateCommonName ? "2.5.4.3" : "2.5.4.11", UniversalTagNumber.UTF8String, "cluster");
        }

        using var certificate = CreateCertificate(new X500DistinguishedName(writer.Encode()));
        _ = await Assert.That(MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId)).IsEqualTo(!duplicateCommonName);
        _ = await Assert.That(nodeId).IsEqualTo(duplicateCommonName ? string.Empty : "node-b");
    }

    /// <summary>Verifies non-string common-name values are refused.</summary>
    [Test]
    public async Task NonStringCommonNameIsRefused()
    {
        var writer = new AsnWriter(AsnEncodingRules.DER);
        using (writer.PushSequence())
        using (writer.PushSetOf())
        using (writer.PushSequence())
        {
            writer.WriteObjectIdentifier("2.5.4.3");
            writer.WriteInteger(42);
        }

        using var certificate = CreateCertificate(new X500DistinguishedName(writer.Encode()));
        _ = await Assert.That(MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId)).IsFalse();
        _ = await Assert.That(nodeId).IsEqualTo(string.Empty);
    }

    private static X509Certificate2 CreateCertificate(string subject) => CreateCertificate(new X500DistinguishedName(subject));

    private static X509Certificate2 CreateCertificate(X500DistinguishedName subject)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
        var now = TimeProvider.System.GetUtcNow();
        return request.CreateSelfSigned(now.AddDays(-1), now.AddDays(1));
    }

    private static void WriteAttribute(AsnWriter writer, string oid, UniversalTagNumber encoding, string value)
    {
        using (writer.PushSequence())
        {
            writer.WriteObjectIdentifier(oid);
            writer.WriteCharacterString(encoding, value);
        }
    }
}
