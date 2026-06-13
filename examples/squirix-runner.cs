#:project ../src/squirix/Squirix.csproj
#:project ../src/squirix.server/Squirix.Server.csproj
#:property TargetFramework=net10.0
#:property PublishAot=false
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Grpc.Core;
using Squirix;
using Squirix.Server;

var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    PrintHelp();
    return 0;
}

var runLoad = !argv.Any(static arg => string.Equals(arg, "--skip-load", StringComparison.OrdinalIgnoreCase));
using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
var cancellationToken = cts.Token;
var previousTestRoot = Environment.GetEnvironmentVariable("SQUIRIX_TEST_ROOT");
var previousCurrentDirectory = Directory.GetCurrentDirectory();
var demoRoot = Path.Join(Path.GetTempPath(), $"squirix-runner-{Guid.NewGuid():N}");
Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", demoRoot);

try
{
    _ = Directory.CreateDirectory(demoRoot);
    var endpoint = $"https://127.0.0.1:{NextFreePort()}";
    WriteSettingsFile(demoRoot, endpoint);
    Directory.SetCurrentDirectory(demoRoot);

    await using var host = await SquirixServer.StartAsync(cancellationToken).ConfigureAwait(false);
    await using var client = await SquirixClient.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
    var defaultCache = await client.GetCacheAsync<object?>("default", cancellationToken).ConfigureAwait(false);
    var users = await client.GetCacheAsync<string>("users", cancellationToken).ConfigureAwait(false);

    await DemoDefaultCacheAsync(defaultCache, cancellationToken).ConfigureAwait(false);
    await DemoTypedNamedCacheAsync(users, cancellationToken).ConfigureAwait(false);

    await Console.Out.WriteLineAsync($"Metrics endpoint available at {endpoint}/metrics").ConfigureAwait(false);

    if (runLoad)
    {
        await Console.Out.WriteLineAsync("Running demo load for up to five minutes. Press Ctrl+C to stop.").ConfigureAwait(false);
        await RunDemoLoadAsync(defaultCache, cancellationToken).ConfigureAwait(false);
    }

    return 0;
}
finally
{
    Directory.SetCurrentDirectory(previousCurrentDirectory);
    Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", previousTestRoot);
    TryDeleteDirectory(demoRoot);
}

static async Task DemoDefaultCacheAsync(ICache<object?> cache, CancellationToken cancellationToken)
{
    await cache.SetAsync(
        "session:42",
        new { Status = "active", LastSeenUtc = DateTime.UtcNow },
        new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(5) },
        cancellationToken).ConfigureAwait(false);

    var session = await cache.GetEntryAsync("session:42", cancellationToken).ConfigureAwait(false);
    await Console.Out.WriteLineAsync($"Default cache -> payload={session.Entry?.Value}").ConfigureAwait(false);

    var touched = await cache.TouchAsync("session:42", TimeSpan.FromMinutes(10), cancellationToken).ConfigureAwait(false);
    var expiration = await cache.GetExpirationAsync("session:42", cancellationToken).ConfigureAwait(false);
    await Console.Out.WriteLineAsync($"TouchAsync -> updated={touched}, expiration={expiration}").ConfigureAwait(false);
}

static async Task DemoTypedNamedCacheAsync(ICache<string> users, CancellationToken cancellationToken)
{
    var added = await users.TryAddAsync("user:42", "created", cancellationToken: cancellationToken).ConfigureAwait(false);
    await Console.Out.WriteLineAsync($"TryAddAsync -> added={added}").ConfigureAwait(false);

    var stored = await users.GetValueAsync("user:42", cancellationToken).ConfigureAwait(false);
    await Console.Out.WriteLineAsync($"GetValueAsync -> found={stored.Found}, value={stored.Value}").ConfigureAwait(false);

    await users.SetAsync("user:42", "updated", cancellationToken: cancellationToken).ConfigureAwait(false);

    var lookup = await users.GetValueAsync("user:42", cancellationToken).ConfigureAwait(false);
    await Console.Out.WriteLineAsync($"GetValueAsync -> present={lookup.Found}").ConfigureAwait(false);

    var removed = await users.RemoveAsync("user:42", cancellationToken).ConfigureAwait(false);
    await Console.Out.WriteLineAsync($"RemoveAsync -> removed={removed}").ConfigureAwait(false);
}

static void PrintHelp()
{
    Console.Out.WriteLine("squirix-runner - file-based demo runner for squirix.");
    Console.Out.WriteLine();
    Console.Out.WriteLine("Usage:");
    Console.Out.WriteLine("  dotnet run --file examples/squirix-runner.cs -- [--skip-load]");
}

static async Task RunDemoLoadAsync(ICache<object?> cache, CancellationToken cancellationToken)
{
    var payload = new string('X', 4 * 1024);

    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            var key = $"load:{RandomNumberGenerator.GetInt32(0, 2048):D4}";
            if (RandomNumberGenerator.GetInt32(0, 10) < 7)
            {
                await cache.SetAsync(
                    key,
                    new { Blob = payload, AtUtc = DateTime.UtcNow, Key = key },
                    new CacheEntryOptions { Expiration = TimeSpan.FromSeconds(RandomNumberGenerator.GetInt32(15, 45)) },
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _ = await cache.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (RpcException)
        {
            // Demo load ignores transient failures while the runtime is being exercised.
        }
        catch (IOException)
        {
            // Demo load ignores transient failures while the runtime is being exercised.
        }

        await Task.Delay(50, cancellationToken).ConfigureAwait(false);
    }
}

static void TryDeleteDirectory(string path)
{
    try
    {
        if (Directory.Exists(path))
            Directory.Delete(path, true);
    }
    catch (IOException)
    {
        // Best-effort cleanup for a demo-only temp directory.
    }
    catch (UnauthorizedAccessException)
    {
        // Best-effort cleanup for a demo-only temp directory.
    }
}

static int NextFreePort()
{
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
}

static void WriteSettingsFile(string directory, string endpoint)
{
    var settings = $$"""
        {
          "Squirix": {
            "Cluster": {
              "NodeId": "runner",
              "Url": "{{endpoint}}",
              "Peers": [
                {
                  "NodeId": "runner",
                  "Url": "{{endpoint}}"
                }
              ]
            }
          }
        }
        """;
    File.WriteAllText(Path.Join(directory, "Squirix.settings.json"), settings);
}
