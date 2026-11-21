using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Exceptions;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Tests for the per-node development certificate of the test kit.</summary>
public sealed class DevelopmentCertificateTests
{
    /// <summary>Ensures every node gets its own copy of the development certificate with the same identity and private key.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    /// <exception cref="SkipTestException">Thrown on macOS, where test hosts use externally configured TLS material.</exception>
    [Test]
    public async Task EveryNodeGetsItsOwnCertificateCopy()
    {
        if (OperatingSystem.IsMacOS())
            throw new SkipTestException("Nodes take no development certificate from the store on macOS.");

        await using var first = BuildServices();
        await using var second = BuildServices();
        var firstCertificate = ResolveNodeCertificate(first);
        var secondCertificate = ResolveNodeCertificate(second);

        _ = await Assert.That(firstCertificate).IsNotNull();
        _ = await Assert.That(secondCertificate).IsNotNull();
        _ = await Assert.That(firstCertificate).IsNotSameReferenceAs(secondCertificate);
        _ = await Assert.That(firstCertificate.Extensions).IsNotSameReferenceAs(secondCertificate.Extensions);
        _ = await Assert.That(firstCertificate.Thumbprint).IsEqualTo(secondCertificate.Thumbprint);
        _ = await Assert.That(firstCertificate.HasPrivateKey).IsTrue();
        _ = await Assert.That(secondCertificate.HasPrivateKey).IsTrue();
    }

    /// <summary>Ensures nodes on macOS take no certificate from the login keychain during test setup.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    /// <exception cref="SkipTestException">Thrown on platforms other than macOS.</exception>
    [Test]
    public async Task MacOsTestNodesAvoidTheLoginKeychain()
    {
        if (!OperatingSystem.IsMacOS())
            throw new SkipTestException("Only macOS skips the store lookup.");

        await using var services = BuildServices();

        _ = await Assert.That(ResolveNodeCertificate(services)).IsNull();
    }

    private static NodeProvider BuildServices()
    {
        var services = new TestServiceCollection();
        if (DevelopmentCertificate.Shared is { } shared)
            _ = services.AddSingleton(_ => new DevelopmentCertificate.NodeCertificate(shared));
        return services.BuildServiceProvider();
    }

    private static X509Certificate2? ResolveNodeCertificate(NodeProvider provider)
        => provider.GetService<DevelopmentCertificate.NodeCertificate>()?.Value;
}
