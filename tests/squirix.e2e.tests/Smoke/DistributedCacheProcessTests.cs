using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Exercises the public distributed cache adapter against a separate HTTPS server process.</summary>
[Property("Suite", "Smoke")]
public sealed class DistributedCacheProcessTests
{
    /// <summary>Verifies binary operations, expiration, and failure after the hosted server stops.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task AdapterReportsUnavailableAfterExit(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(false, cancellationToken);
        await using var provider = CreateProvider(server.Uri);
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

    /// <summary>Verifies that a fresh HybridCache provider uses the previously stored secondary value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task FreshHybridCacheUsesServerL2(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(false, cancellationToken);
        await using var first = CreateProvider(server.Uri);
        var writer = first.GetRequiredService<HybridCache>();
        var expected = await writer.GetOrCreateAsync("shared", static _ => ValueTask.FromResult("from-server"), cancellationToken: cancellationToken);
        var distributed = first.GetRequiredService<IDistributedCache>();
        using var writeBound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        writeBound.CancelAfter(TimeSpan.FromSeconds(5));
        while (await distributed.GetAsync("shared", writeBound.Token) == null)
            await Task.Delay(TimeSpan.FromMilliseconds(25), TimeProvider.System, writeBound.Token);

        await using var second = CreateProvider(server.Uri);
        var reader = second.GetRequiredService<HybridCache>();
        var actual = await reader.GetOrCreateAsync("shared", static _ => ValueTask.FromResult("unexpected-factory"), cancellationToken: cancellationToken);
        _ = await Assert.That(actual).IsEqualTo(expected);
    }

    /// <summary>A warm write takes ownership of its wire bytes before returning to the caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task WarmSetOwnsItsPayload(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(false, cancellationToken);
        await using var provider = CreateProvider(server.Uri);
        var cache = provider.GetRequiredService<IDistributedCache>();
        _ = await cache.GetAsync("warm", cancellationToken);
        byte[] bytes = [0, 255, 128];
        var write = cache.SetAsync("owned", bytes, new DistributedCacheEntryOptions(), CancellationToken.None);
        bytes[0] = 42;
        await write;
        var stored = await cache.GetAsync("owned", cancellationToken);
        _ = await Assert.That(stored.AsSpan().SequenceEqual(new byte[] { 0, 255, 128 })).IsTrue();
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

    private static ServiceProvider CreateProvider(Uri uri)
    {
        var services = new ServiceCollection();
        _ = services.AddSquirixCache(options => options.ClientOptions.Endpoints.Add(uri));
        _ = services.AddHybridCache();
        return services.BuildServiceProvider();
    }
}
