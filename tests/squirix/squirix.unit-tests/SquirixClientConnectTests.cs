using System;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Covers the public remote-only client factory surface.
/// </summary>
public sealed class SquirixClientConnectTests
{
    /// <summary>
    /// Verifies explicit remote mode requires at least one endpoint.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ConnectAsyncOptionsRejectNoEndpoints()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(static () => SquirixClient.ConnectAsync(static _ => { }, TestContext.Current.CancellationToken).AsTask());

        Assert.Contains("endpoint", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Verifies plaintext HTTP endpoints are rejected during bootstrap configuration.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ConnectAsyncOptionsRejectPlaintextHttpEndpoint()
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(static () =>
            SquirixClient.ConnectAsync(static options => options.Endpoints.Add("http://127.0.0.1:1"), TestContext.Current.CancellationToken).AsTask());

        Assert.Contains("HTTPS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Verifies the string overload rejects plaintext HTTP endpoints.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ConnectAsyncRejectsPlaintextHttpEndpoint()
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(static () => SquirixClient.ConnectAsync("http://127.0.0.1:1", TestContext.Current.CancellationToken).AsTask());

        Assert.Contains("HTTPS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
