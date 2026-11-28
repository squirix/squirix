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
    /// <summary>Requires an absolute TCP or HTTPS address at every SDK bootstrap endpoint.</summary>
    /// <param name="address">The endpoint address.</param>
    [Test]
    [Arguments("http://localhost:7443")]
    [Arguments("relative")]
    [Arguments("tcp://localhost")]
    public async Task RejectsInvalidEndpoint(string address)
    {
        var error = TestKit.ExceptionAssert.For<ArgumentException>().Throws(
            new Uri(address, UriKind.RelativeOrAbsolute),
            BinaryCacheClient.ValidateEndpoint);

        _ = await Assert.That(error.ParamName).IsEqualTo("uri");
    }
}
