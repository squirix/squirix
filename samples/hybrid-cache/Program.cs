using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;

namespace Squirix.HybridCacheSample;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (args is not [var key] || string.Equals(key, "--help", StringComparison.Ordinal))
        {
            await Console.Error.WriteLineAsync("Usage: <key>").ConfigureAwait(false);
            return args is ["--help"] ? 0 : 2;
        }

        var endpoint = Environment.GetEnvironmentVariable("Squirix__Endpoint") is { Length: > 0 } configuredEndpoint
            ? new Uri(configuredEndpoint)
            : new UriBuilder(Uri.UriSchemeHttps, "localhost", 5001).Uri;
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
        var cache = provider.GetRequiredService<HybridCache>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var policy = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromMinutes(5),
            LocalCacheExpiration = TimeSpan.FromSeconds(2),
        };
        var value = await cache.GetOrCreateAsync(
            $"message:{key}",
            static _ => ValueTask.FromResult($"created by process {Environment.ProcessId}"),
            policy,
            cancellationToken: timeout.Token).ConfigureAwait(false);
        await Console.Out.WriteLineAsync(value).ConfigureAwait(false);
        return 0;
    }
}
