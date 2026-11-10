using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Verifies distributed cache behavior when its server process is stopped.</summary>
[Property("Suite", "Smoke")]
public sealed class DistributedCacheProcessShutdownTests
{
    /// <summary>Verifies binary operations, expiration, and failure after the hosted server stops.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task AdapterReportsUnavailableAfterExit(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(false, cancellationToken);
        await using var provider = DistributedCacheProcessFixture.CreateProvider(server.Uri);
        var cache = provider.GetRequiredService<IDistributedCache>();
        byte[] bytes = [0, 255, 128];
        var policy = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30),
            SlidingExpiration = TimeSpan.FromSeconds(15),
        };
        await cache.SetAsync("binary", bytes, policy, cancellationToken);
        await cache.RefreshAsync("binary", cancellationToken);
        var result = await cache.GetAsync("binary", cancellationToken);
        _ = await Assert.That(result.AsSpan().SequenceEqual(bytes)).IsTrue();
        await cache.RemoveAsync("binary", cancellationToken);
        _ = await Assert.That(await cache.GetAsync("binary", cancellationToken)).IsNull();

        var absolute = new DistributedCacheEntryOptions { AbsoluteExpiration = TimeProvider.System.GetUtcNow().AddMilliseconds(500) };
        await cache.SetAsync("expires", bytes, absolute, cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(600), TimeProvider.System, cancellationToken);
        _ = await Assert.That(await cache.GetAsync("expires", cancellationToken)).IsNull();

        await server.StopAsync(cancellationToken);
        _ = await Assert.That(await ProbeAsync(cache, cancellationToken)).IsEqualTo(StatusCode.Unavailable);
    }

    private static async Task<StatusCode> ProbeAsync(IDistributedCache cache, CancellationToken cancellationToken)
    {
        try
        {
            _ = await cache.GetAsync("binary", cancellationToken).ConfigureAwait(false);
            return StatusCode.OK;
        }
        catch (RpcException ex)
        {
            return ex.StatusCode;
        }
    }
}
