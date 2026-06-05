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
    /// Verifies the options overload also fails when server is unreachable.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ConnectAsyncOptionsThrowsWhenServerUnreachable()
    {
        _ = await Assert.ThrowsAnyAsync<Exception>(static () =>
            SquirixClient.ConnectAsync(static options => options.Endpoints.Add("http://127.0.0.1:1"), TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Verifies ConnectAsync fails when the server is unreachable.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ConnectAsyncThrowsWhenServerUnreachable() => _ =
        await Assert.ThrowsAnyAsync<Exception>(static () => SquirixClient.ConnectAsync("http://127.0.0.1:1", TestContext.Current.CancellationToken).AsTask());
}
