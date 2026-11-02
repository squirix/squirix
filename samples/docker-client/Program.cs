using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
var token = CreateDevelopmentToken();
await using var client = await SquirixClient.ConnectAsync(
    options =>
    {
        options.Endpoints.Add(new Uri("https://squirix-node-a:5000"));
        options.Endpoints.Add(new Uri("https://squirix-node-b:5000"));
        options.Endpoints.Add(new Uri("https://squirix-node-c:5000"));
        options.BearerTokenProvider = _ => new ValueTask<string>(token);
    },
    timeout.Token);

var cache = await client.GetCacheAsync<string>("docker-demo", timeout.Token);
var key = "greeting-" + Guid.NewGuid().ToString("N");
await cache.SetAsync(key, "hello from Docker", cancellationToken: timeout.Token);
var result = await cache.GetValueAsync(key, timeout.Token);
if (!result.Found || result.Value != "hello from Docker")
    throw new InvalidOperationException("The cache did not return the value that was written.");

Console.WriteLine($"Found={result.Found}; Value={result.Value}");

static string CreateDevelopmentToken()
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

static string Base64Url(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
