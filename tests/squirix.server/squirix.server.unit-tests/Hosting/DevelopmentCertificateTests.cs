using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Tests for the per-node development certificate of the test kit.</summary>
public sealed class DevelopmentCertificateTests
{
    /// <summary>Ensures every node gets its own copy of the development certificate with the same identity and private key.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task EveryNodeGetsItsOwnCertificateCopy()
    {
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

    private static ServiceProvider BuildServices()
    {
        var services = new ServiceCollection();
        DevelopmentCertificate.AddTo(services);
        return services.BuildServiceProvider();
    }

    private static X509Certificate2? ResolveNodeCertificate(ServiceProvider provider)
        => provider.GetService<DevelopmentCertificate.NodeCertificate>()?.Value;
}
