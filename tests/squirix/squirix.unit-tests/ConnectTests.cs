using System;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Covers the public remote-only client factory surface.</summary>
public sealed class ConnectTests : UnitTestBase
{
    /// <summary>Verifies explicit remote mode requires at least one endpoint.</summary>
    [Fact]
    public async Task ConnectAsyncOptionsRejectNoEndpoints()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(static () =>
            SquirixClient.ConnectAsync(static _ => { }, DefaultCancellationToken).AsTask());

        Assert.Contains("endpoint", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Verifies plaintext HTTP endpoints are rejected during bootstrap configuration.</summary>
    [Fact]
    public async Task ConnectAsyncOptionsRejectPlaintextHttpEndpoint()
    {
        var ex = await AsyncAssert.ThrowsAsync<ArgumentException, ISquirixClient>(
            SquirixClient.ConnectAsync(static options => options.Endpoints.Add(new Uri("http://127.0.0.1:1")), DefaultCancellationToken));

        Assert.Contains("HTTPS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Verifies the Uri overload rejects plaintext HTTP endpoints.</summary>
    [Fact]
    public async Task ConnectAsyncRejectsPlaintextHttpEndpoint()
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(static () =>
            SquirixClient.ConnectAsync("http://127.0.0.1:1", DefaultCancellationToken).AsTask());

        Assert.Contains("HTTPS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
