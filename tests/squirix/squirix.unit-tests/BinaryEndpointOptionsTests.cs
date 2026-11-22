using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers endpoint validation before the binary client opens a socket.</summary>
[Immutable]
public sealed class BinaryEndpointOptionsTests
{
    /// <summary>Requires an absolute URI and TLS at every SDK bootstrap endpoint.</summary>
    /// <param name="address">The endpoint address.</param>
    [Test]
    [Arguments("http://localhost:7443")]
    [Arguments("relative")]
    public async Task RefusesEndpointsWithoutAbsoluteTls(string address)
    {
        var error = TestKit.ExceptionAssert.For<ArgumentException>().Throws(
            new Uri(address, UriKind.RelativeOrAbsolute),
            BinaryCacheClient.RequireHttps);

        _ = await Assert.That(error.ParamName).IsEqualTo("uri");
    }
}
