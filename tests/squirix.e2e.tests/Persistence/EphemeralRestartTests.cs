using System.Threading.Tasks;
using Squirix.E2ETests.Support;
using Squirix.E2ETests.Support.Restart;
using Xunit;

namespace Squirix.E2ETests.Persistence;

/// <summary>
/// Verifies ephemeral nodes do not restore cache state across restart.
/// </summary>
public sealed class EphemeralRestartTests : EndToEndTestBase
{
    /// <summary>
    /// Ensures a restarted ephemeral node does not restore previously written values.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task RestartShouldNotRestoreValueInEphemeralMode()
    {
        await using var node = await EphemeralRestartableSingleNode.StartAsync(DefaultCancellationToken);
        var cache = await node.GetCacheAsync<string>("ephemeral-restart", DefaultCancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: DefaultCancellationToken);

        await node.RestartAsync(DefaultCancellationToken);

        cache = await node.GetCacheAsync<string>("ephemeral-restart", DefaultCancellationToken);
        var result = await cache.GetValueAsync("key", DefaultCancellationToken);
        Assert.False(result.Found);
    }
}
