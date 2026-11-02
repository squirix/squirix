using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;

namespace Squirix.DockerClient;

internal static class Program
{
    private static async Task Main()
    {
        var endpoints = Environment.GetEnvironmentVariable("SQUIRIX_CLIENT_ENDPOINTS");
        if (string.IsNullOrWhiteSpace(endpoints))
            throw new InvalidOperationException("Set SQUIRIX_CLIENT_ENDPOINTS to semicolon-separated HTTPS endpoints.");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = CreateDevelopmentToken();
        var client = await SquirixClient.ConnectAsync(
            options =>
            {
                foreach (var endpoint in endpoints.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                    options.Endpoints.Add(new Uri(endpoint));
                options.BearerTokenProvider = _ => new ValueTask<string>(token);
            },
            timeout.Token).ConfigureAwait(false);

        await using (client.ConfigureAwait(false))
        {
            var cache = await client.GetCacheAsync<string>("docker-demo", timeout.Token).ConfigureAwait(false);
            var key = "greeting-" + Guid.NewGuid().ToString("N");
            await cache.SetAsync(key, "hello from Docker", cancellationToken: timeout.Token).ConfigureAwait(false);
            var result = await cache.GetValueAsync(key, timeout.Token).ConfigureAwait(false);
            if (!result.Found || !string.Equals(result.Value, "hello from Docker", StringComparison.Ordinal))
                throw new InvalidOperationException("The cache did not return the value that was written.");

            Console.WriteLine($"Found={result.Found}; Value={result.Value}");
        }
    }

    private static string CreateDevelopmentToken()
    {
        // These public credentials match docker-compose.yml and are for local development only.
        const string signingKey = "ZGV2LXNxdWlyaXgtZG9ja2VyLWp3dC1rZXkhISEhISE=";
        var issuedAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var expiresAt = issuedAt + 300;
        var header = Base64Url(Encoding.UTF8.GetBytes("{\"alg\":\"HS256\",\"typ\":\"JWT\"}"));
        var payload = Base64Url(Encoding.UTF8.GetBytes(
            $"{{\"iss\":\"https://squirix.docker.dev\",\"aud\":\"squirix\",\"sub\":\"docker-client\",\"iat\":{issuedAt},\"exp\":{expiresAt}}}"));
        var unsignedToken = header + "." + payload;
        var signature = HMACSHA256.HashData(Convert.FromBase64String(signingKey), Encoding.ASCII.GetBytes(unsignedToken));
        return unsignedToken + "." + Base64Url(signature);
    }

    private static string Base64Url(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
