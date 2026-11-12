using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Squirix.Client;

namespace Squirix.AspNetSharedCache;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var app = builder.Build();

        var endpoint = app.Configuration["Squirix:Endpoint"] ?? "https://localhost:5001";
        using var startupTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var client = await SquirixClient.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(new Uri(endpoint));
                options.JsonSerializerContexts.Add(BannerJsonContext.Default);
            },
            startupTimeout.Token).ConfigureAwait(false);
        await using var clientLifetime = client.ConfigureAwait(false);
        var cache = await client.GetCacheAsync<Banner>("shared-banner-demo", startupTimeout.Token).ConfigureAwait(false);

        _ = app.MapPut(
            "/banner/{key}",
            async (string key, Banner banner, CancellationToken cancellationToken) =>
            {
                await cache.SetAsync(key, banner, null, cancellationToken).ConfigureAwait(false);
                return Results.NoContent();
            });

        _ = app.MapGet(
            "/banner/{key}",
            async (string key, CancellationToken cancellationToken) =>
            {
                var result = await cache.GetValueAsync(key, cancellationToken).ConfigureAwait(false);
                return result.Found ? Results.Ok(result.Value) : Results.NotFound();
            });

        await app.RunAsync().ConfigureAwait(false);
    }
}
