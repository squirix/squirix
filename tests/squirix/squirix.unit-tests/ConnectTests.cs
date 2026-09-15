using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers the public remote-only client factory surface.</summary>
[Immutable]
public sealed class ConnectTests : UnitTestBase
{
    /// <summary>Verifies explicit remote mode requires at least one endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectAsyncOptionsRejectNoEndpoints(CancellationToken cancellationToken)
    {
        var ex = await AsyncAssert.ThrowsAsync<InvalidOperationException, ISquirixClient>(SquirixClient.ConnectAsync(static _ => { }, cancellationToken));

        _ = await Assert.That(ex.Message).Contains("endpoint", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Verifies the Uri overload rejects plaintext HTTP endpoints.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectAsyncRejectsPlaintextHttpEndpoint(CancellationToken cancellationToken)
    {
        var ex = await AsyncAssert.ThrowsAsync<ArgumentException, ISquirixClient>(SquirixClient.ConnectAsync(new Uri("http://127.0.0.1:1"), cancellationToken));

        _ = await Assert.That(ex.Message).Contains("HTTPS", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Verifies plaintext HTTP endpoints are rejected during bootstrap configuration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OptionsRejectPlaintextHttpEndpoint(CancellationToken cancellationToken)
    {
        var ex = await AsyncAssert.ThrowsAsync<ArgumentException, ISquirixClient>(
            SquirixClient.ConnectAsync(static options => options.Endpoints.Add(new Uri("http://127.0.0.1:1")), cancellationToken));

        _ = await Assert.That(ex.Message).Contains("HTTPS", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Verifies relative endpoints are rejected as non-absolute server URIs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OptionsRejectRelativeEndpoint(CancellationToken cancellationToken)
    {
        var ex = await AsyncAssert.ThrowsAsync<ArgumentException, ISquirixClient>(
            SquirixClient.ConnectAsync(static options => options.Endpoints.Add(new Uri("not-absolute", UriKind.Relative)), cancellationToken));

        _ = await Assert.That(ex.Message).Contains("absolute Squirix server URI", StringComparison.Ordinal);
    }
}
