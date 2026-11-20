using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;

namespace Squirix.SharedCacheSample;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        var put = args is ["put", _, _];
        if (!put && args is not ["get", _])
        {
            await Console.Error.WriteLineAsync("Usage: put <key> <message> | get <key>".AsMemory(), CancellationToken.None).ConfigureAwait(false);
            return args is ["--help"] ? 0 : 2;
        }

        var endpoint = Environment.GetEnvironmentVariable("Squirix__Endpoint") is { Length: > 0 } configuredEndpoint
            ? new Uri(configuredEndpoint)
            : new UriBuilder(Uri.UriSchemeHttps, "localhost", 5001).Uri;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var client = await SquirixClient.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(endpoint);
                options.JsonSerializerContexts.Add(BannerJsonContext.Default);
                if (Environment.GetEnvironmentVariable("SQUIRIX_SAMPLE_BEARER_TOKEN") is { Length: > 0 } token)
                    options.BearerTokenProvider = _ => ValueTask.FromResult(token);
            },
            timeout.Token).ConfigureAwait(false);
        await using var lifetime = client.ConfigureAwait(false);
        var cache = await client.GetCacheAsync<Banner>("shared-banner-demo", timeout.Token).ConfigureAwait(false);

        if (put)
        {
            await cache.SetAsync(args[1], new Banner(args[2]), null, timeout.Token).ConfigureAwait(false);
            return 0;
        }

        var result = await cache.GetValueAsync(args[1], timeout.Token).ConfigureAwait(false);
        if (!result.Found || result.Value is not { } banner)
        {
            await Console.Error.WriteLineAsync("Key not found.".AsMemory(), timeout.Token).ConfigureAwait(false);
            return 1;
        }

        await Console.Out.WriteLineAsync(banner.Message.AsMemory(), timeout.Token).ConfigureAwait(false);
        return 0;
    }
}
