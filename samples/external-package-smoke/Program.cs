using System;
using System.Diagnostics.CodeAnalysis;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;
using Squirix.Client;
using Squirix.Server;

namespace Squirix.ExternalPackageSmoke;

internal static class Program
{
    private const string IsolationSharedKey = "shared-key";

    private static async Task<Uri> LoadConfiguredEndpointAsync(CancellationToken cancellationToken)
    {
        var json = await File.ReadAllTextAsync("appsettings.json", cancellationToken).ConfigureAwait(false);
        using var document = JsonDocument.Parse(json);
        var uri = document.RootElement.GetProperty("Squirix").GetProperty("Cluster").GetProperty("Uri").GetString() ?? ThrowMissingClusterUri();
        return new Uri(uri, UriKind.Absolute);
    }

    private static async Task<int> Main()
    {
        var testRoot = Directory.CreateTempSubdirectory("squirix-external-smoke");
        try
        {
            Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", testRoot.FullName);
            Environment.SetEnvironmentVariable("SQUIRIX_MANAGEMENT_URI", "https://127.0.0.1:0");

            var endpoint = await LoadConfiguredEndpointAsync(CancellationToken.None).ConfigureAwait(false);
            var server = await SquirixServer.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await using (server.ConfigureAwait(false))
            {
                var client = await SquirixClient.ConnectAsync(CancellationToken.None).ConfigureAwait(false);
                await using var clientLifetime = client.ConfigureAwait(false);
                await RunIsolationAsync(client, CancellationToken.None).ConfigureAwait(false);
                await RunExpirationAsync(client, CancellationToken.None).ConfigureAwait(false);
            }

            await RunDistributedAsync(endpoint, CancellationToken.None).ConfigureAwait(false);

            return 0;
        }
        finally
        {
            testRoot.Delete(true);
        }
    }

    private static async Task RunDistributedAsync(Uri endpoint, CancellationToken cancellationToken)
    {
        var bearerToken = ConfigureJwtAuthentication();
        var server = await SquirixServer.StartAsync(cancellationToken).ConfigureAwait(false);
        await using var serverLifetime = server.ConfigureAwait(false);
        var services = new ServiceCollection();
        _ = services.AddSquirixCache(options =>
        {
            options.ClientOptions.Endpoints.Add(endpoint);
            options.ClientOptions.BearerTokenProvider = ct => GetTokenAsync(bearerToken, ct);
        });
        _ = services.AddHybridCache();
        var first = services.BuildServiceProvider();
        await using var firstLifetime = first.ConfigureAwait(false);
        var distributed = first.GetRequiredService<IDistributedCache>();
        RunSyncOperations(distributed);
        var policy = new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(1) };
        await distributed.SetAsync("bytes", [0, 255], policy, cancellationToken).ConfigureAwait(false);
        await distributed.RefreshAsync("bytes", cancellationToken).ConfigureAwait(false);
        var bytes = await distributed.GetAsync("bytes", cancellationToken).ConfigureAwait(false);
        if (!bytes.AsSpan().SequenceEqual(new byte[] { 0, 255 }))
            throw new InvalidOperationException("The distributed cache changed the binary payload.");

        await distributed.RemoveAsync("bytes", cancellationToken).ConfigureAwait(false);
        if (await distributed.GetAsync("bytes", cancellationToken).ConfigureAwait(false) != null)
            throw new InvalidOperationException("The distributed cache retained the removed key.");

        const string key = "hybrid";
        var writer = first.GetRequiredService<HybridCache>();
        var written = await writer.GetOrCreateAsync(key, static _ => ValueTask.FromResult("from-squirix"), cancellationToken: cancellationToken).ConfigureAwait(false);
        using var writeTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        writeTimeout.CancelAfter(TimeSpan.FromSeconds(5));
        while (await distributed.GetAsync(key, writeTimeout.Token).ConfigureAwait(false) == null)
            await Task.Delay(TimeSpan.FromMilliseconds(25), TimeProvider.System, writeTimeout.Token).ConfigureAwait(false);
        var second = services.BuildServiceProvider();
        await using var secondLifetime = second.ConfigureAwait(false);
        var reader = second.GetRequiredService<HybridCache>();
        var read = await reader.GetOrCreateAsync(key, static _ => ValueTask.FromResult("factory-ran"), cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!string.Equals(written, "from-squirix", StringComparison.Ordinal) || !string.Equals(read, written, StringComparison.Ordinal))
            throw new InvalidOperationException("A fresh HybridCache instance did not read the value from Squirix.");
    }

    private static string ConfigureJwtAuthentication()
    {
        const string issuer = "external-package-smoke";
        const string audience = "squirix-cache";
        var key = RandomNumberGenerator.GetBytes(32);
        Environment.SetEnvironmentVariable("SQUIRIX_JWT_AUTHORITY", null);
        Environment.SetEnvironmentVariable("SQUIRIX_JWT_ISSUER", issuer);
        Environment.SetEnvironmentVariable("SQUIRIX_JWT_AUDIENCE", audience);
        Environment.SetEnvironmentVariable("SQUIRIX_JWT_SIGNING_KEY", Convert.ToBase64String(key));

        var credentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256);
        var now = DateTime.UtcNow;
        var token = new JwtSecurityToken(issuer, audience, notBefore: now, expires: now.AddMinutes(5), signingCredentials: credentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private static async ValueTask<string> GetTokenAsync(string bearerToken, CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding | ConfigureAwaitOptions.ContinueOnCapturedContext);
        cancellationToken.ThrowIfCancellationRequested();
        return bearerToken;
    }

    [SuppressMessage("Usage", "MA0045", Justification = "The smoke test exercises the synchronous IDistributedCache contract.")]
    private static void RunSyncOperations(IDistributedCache distributed)
    {
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(new RejectPostingContext());
        try
        {
            distributed.Set("sync", [42], new DistributedCacheEntryOptions());
            if (!distributed.Get("sync").AsSpan().SequenceEqual(new byte[] { 42 }))
                throw new InvalidOperationException("A synchronous warm Get changed the payload.");

            distributed.Remove("sync");
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    private static async Task RunExpirationAsync(ISquirixClient client, CancellationToken ct)
    {
        var cache = await client.GetCacheAsync<string>("smoke-expiration", ct).ConfigureAwait(false);
        await cache.SetAsync("expiring", "x", new CacheEntryOptions { Expiration = TimeSpan.FromMilliseconds(80) }, ct).ConfigureAwait(false);
        await Task.Delay(200, ct).ConfigureAwait(false);
        var result = await cache.GetValueAsync("expiring", ct).ConfigureAwait(false);
        if (result.Found)
            throw new InvalidOperationException("Expected expiration key to be absent after wait.");
    }

    private static async Task RunIsolationAsync(ISquirixClient client, CancellationToken ct)
    {
        var a = await client.GetCacheAsync<string>("smoke-a", ct).ConfigureAwait(false);
        var b = await client.GetCacheAsync<string>("smoke-b", ct).ConfigureAwait(false);
        await a.SetAsync(IsolationSharedKey, "from-a", cancellationToken: ct).ConfigureAwait(false);
        await b.SetAsync(IsolationSharedKey, "from-b", cancellationToken: ct).ConfigureAwait(false);
        var v1 = (await a.GetValueAsync(IsolationSharedKey, ct).ConfigureAwait(false)).Value;
        var v2 = (await b.GetValueAsync(IsolationSharedKey, ct).ConfigureAwait(false)).Value;
        if (!string.Equals(v1, "from-a", StringComparison.Ordinal) || !string.Equals(v2, "from-b", StringComparison.Ordinal))
            throw new InvalidOperationException("Named cache isolation failed.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string ThrowMissingClusterUri() => throw new InvalidOperationException("appsettings.json does not contain Squirix:Cluster:Uri.");

    private sealed class RejectPostingContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object? state) =>
            throw new InvalidOperationException("The synchronous cache operation tried to resume on the blocked caller context.");
    }
}
