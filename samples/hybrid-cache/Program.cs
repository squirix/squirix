using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using HybridCacheService = Microsoft.Extensions.Caching.Hybrid.HybridCache;

namespace Squirix.HybridCache;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        var write = args is ["set", _];
        if (!write && args is not ["get", _])
        {
            await Console.Error.WriteLineAsync("Usage: set <key> | get <key>".AsMemory(), CancellationToken.None).ConfigureAwait(false);
            return args is ["--help"] ? 0 : 2;
        }

        var endpoint = Environment.GetEnvironmentVariable("Squirix__Endpoint") is { Length: > 0 } configuredEndpoint
            ? new Uri(configuredEndpoint)
            : new UriBuilder("tcp", "localhost", 5001).Uri;
        var services = new ServiceCollection();
        _ = services.AddSquirixCache(options =>
        {
            options.ClientOptions.Endpoints.Add(endpoint);
            if (Environment.GetEnvironmentVariable("SQUIRIX_SAMPLE_BEARER_TOKEN") is { Length: > 0 } token)
                options.ClientOptions.BearerTokenProvider = _ => ValueTask.FromResult(token);
        });
        _ = services.AddHybridCache();

        var provider = services.BuildServiceProvider();
        await using var lifetime = provider.ConfigureAwait(false);
        var cache = provider.GetRequiredService<HybridCacheService>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var policy = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromMinutes(5),
            LocalCacheExpiration = TimeSpan.FromSeconds(2),
        };
        var cacheKey = $"message:{args[1]}";
        if (write)
        {
            var value = $"created by process {Environment.ProcessId}";
            await cache.SetAsync(cacheKey, value, policy, cancellationToken: timeout.Token).ConfigureAwait(false);
            await Console.Out.WriteLineAsync(value.AsMemory(), timeout.Token).ConfigureAwait(false);
            return 0;
        }

        var existing = await cache.GetOrCreateAsync(
            cacheKey,
            static _ => ValueTask.FromResult("cache miss"),
            policy,
            cancellationToken: timeout.Token).ConfigureAwait(false);
        await Console.Out.WriteLineAsync(existing.AsMemory(), timeout.Token).ConfigureAwait(false);
        return 0;
    }
}
