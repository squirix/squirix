using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;

namespace Squirix.AspNetHybridCache;

internal static class Program
{
    private static Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var configuredEndpoint = builder.Configuration["CacheEndpoint"];
        if (string.IsNullOrWhiteSpace(configuredEndpoint))
            throw new InvalidOperationException("Configure CacheEndpoint.");

        var endpoint = new Uri(configuredEndpoint);
        _ = builder.Services.AddSquirixCache(options =>
            options.ClientOptions.Endpoints.Add(endpoint));
        _ = builder.Services.AddHybridCache();

        var app = builder.Build();
        var policy = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromMinutes(5),
            LocalCacheExpiration = TimeSpan.FromSeconds(2),
        };
        _ = app.MapGet("/messages/{key}", (string key, HybridCache cache, CancellationToken token) =>
            cache.GetOrCreateAsync(
                $"message:{key}",
                static _ => ValueTask.FromResult($"created by process {Environment.ProcessId}"),
                policy,
                cancellationToken: token));
        return app.RunAsync();
    }
}
