using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Xunit;

namespace Squirix.E2ETests.PublicApi.Persistence;

/// <summary>
/// Verifies ephemeral nodes do not restore cache state across restart.
/// </summary>
public sealed class EphemeralRestartTests
{
    /// <summary>
    /// Ensures a restarted ephemeral node does not restore previously written values.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RestartShouldNotRestoreValueInEphemeralMode()
    {
        await using var node = await EphemeralRestartableSingleNode.StartAsync(TestContext.Current.CancellationToken);
        var cache = await node.GetCacheAsync<string>("ephemeral-restart", TestContext.Current.CancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: TestContext.Current.CancellationToken);

        await node.RestartAsync(TestContext.Current.CancellationToken);

        cache = await node.GetCacheAsync<string>("ephemeral-restart", TestContext.Current.CancellationToken);
        var result = await cache.GetValueAsync("key", TestContext.Current.CancellationToken);
        Assert.False(result.Found);
    }
}
