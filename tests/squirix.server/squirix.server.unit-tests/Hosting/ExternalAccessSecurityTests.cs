using System.Net;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers how the listen host maps to the exact loopback address Kestrel binds.</summary>
[Immutable]
public sealed class ExternalAccessSecurityTests : ServerUnitTestBase
{
    /// <summary>A loopback IP literal resolves to exactly that address.</summary>
    /// <param name="host">The listen URI host.</param>
    /// <param name="expected">The expected address text.</param>
    [Test]
    [Arguments("127.0.0.1", "127.0.0.1")]
    [Arguments("127.0.0.2", "127.0.0.2")]
    [Arguments("[::1]", "::1")]
    [Arguments("::1", "::1")]
    public async Task LoopbackIpLiteralResolvesToExactAddress(string host, string expected)
    {
        var resolved = ExternalAccessSecurity.TryGetLoopbackAddress(host, out var address);

        _ = await Assert.That(resolved).IsTrue();
        _ = await Assert.That(address).IsEqualTo(IPAddress.Parse(expected));
    }

    /// <summary>A name or a non-loopback address does not resolve to a loopback address.</summary>
    /// <param name="host">The listen URI host.</param>
    [Test]
    [Arguments("localhost")]
    [Arguments("0.0.0.0")]
    [Arguments("10.0.0.5")]
    [Arguments("squirix-node")]
    public async Task NameOrNonLoopbackAddressDoesNotResolve(string host)
    {
        var resolved = ExternalAccessSecurity.TryGetLoopbackAddress(host, out var address);

        _ = await Assert.That(resolved).IsFalse();
        _ = await Assert.That(address).IsNull();
    }
}
