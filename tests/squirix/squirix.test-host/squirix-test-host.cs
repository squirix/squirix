#:project ../../src/squirix/Squirix.csproj
#:project ../../src/squirix.server/Squirix.Server.csproj
#:property TargetFramework=net10.0
#:property PublishAot=false
using System.Globalization;
using System.Text.Json;
using Squirix;
using Squirix.Server;

var requestedWorkingDirectory = Environment.GetEnvironmentVariable("SQUIRIX_TEST_HOST_WORKING_DIRECTORY");
if (!string.IsNullOrWhiteSpace(requestedWorkingDirectory))
    Directory.SetCurrentDirectory(requestedWorkingDirectory);

using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
await RunCommandAsync(Environment.GetCommandLineArgs().Skip(1).ToArray(), cts.Token);
return;

static async ValueTask RunCommandAsync(string[] args, CancellationToken cancellationToken)
{
    if (args.Length == 0)
        throw new ArgumentException("A test host command is required.");

    switch (args[0])
    {
        case "embedded-write-int":
            if (args.Length != 4)
                throw new ArgumentException("Usage: embedded-write-int <cacheName> <key> <value>");

            await using (var unused = await SquirixServer.StartAsync(cancellationToken))
            await using (var client = await SquirixClient.ConnectAsync(LoadConfiguredEndpoint(), cancellationToken))
            {
                var cache = await client.GetCacheAsync<int>(args[1], cancellationToken);
                var value = int.Parse(args[3], CultureInfo.InvariantCulture);
                await cache.SetAsync(args[2], value, cancellationToken: cancellationToken);

                var persisted = await cache.GetValueAsync(args[2], cancellationToken);
                if (!persisted.Found || persisted.Value != value)
                {
                    throw new InvalidOperationException(
                        $"Expected write verification for key '{args[2]}' to return value {value.ToString(CultureInfo.InvariantCulture)}, but it did not.");
                }
            }

            return;

        case "embedded-read-int":
            if (args.Length != 4)
                throw new ArgumentException("Usage: embedded-read-int <cacheName> <key> <expectedValue>");

            await using (var unused = await SquirixServer.StartAsync(cancellationToken))
            await using (var client = await SquirixClient.ConnectAsync(LoadConfiguredEndpoint(), cancellationToken))
            {
                var cache = await client.GetCacheAsync<int>(args[1], cancellationToken);
                var expectedValue = int.Parse(args[3], CultureInfo.InvariantCulture);
                var read = await cache.GetValueAsync(args[2], cancellationToken);
                if (!read.Found)
                    throw new InvalidOperationException($"Expected key '{args[2]}' to exist after restart, but it was missing.");

                if (read.Value != expectedValue)
                {
                    throw new InvalidOperationException(
                        $"Expected {expectedValue.ToString(CultureInfo.InvariantCulture)} but got {read.Value.ToString(CultureInfo.InvariantCulture)}.");
                }
            }

            return;

        case "default-type-binding-allows-mixed-facades":
            if (args.Length != 2)
                throw new ArgumentException("Usage: default-type-binding-allows-mixed-facades <cacheName>");

            await using (var unused = await SquirixServer.StartAsync(cancellationToken))
            await using (var client = await SquirixClient.ConnectAsync(LoadConfiguredEndpoint(), cancellationToken))
            {
                _ = await client.GetCacheAsync<int>(args[1], cancellationToken);
                _ = await client.GetCacheAsync<long>(args[1], cancellationToken);
            }

            return;

        case "relaxed-mixed-read-fails":
            if (args.Length != 2)
                throw new ArgumentException("Usage: relaxed-mixed-read-fails <cacheName>");

            await using (var unused = await SquirixServer.StartAsync(cancellationToken))
            await using (var client = await SquirixClient.ConnectAsync(LoadConfiguredEndpoint(), cancellationToken))
            {
                var writer = await client.GetCacheAsync<string>(args[1], cancellationToken);
                var reader = await client.GetCacheAsync<long>(args[1], cancellationToken);
                await writer.SetAsync("k", "forty-two", cancellationToken: cancellationToken);

                try
                {
                    _ = await reader.GetValueAsync("k", cancellationToken);
                    throw new InvalidOperationException("Expected incompatible read failure.");
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("System.String", StringComparison.Ordinal)
                                                           && ex.Message.Contains("System.Int64", StringComparison.Ordinal))
                {
                    return;
                }
            }

        default:
            throw new ArgumentException($"Unknown command '{args[0]}'.");
    }
}

static string LoadConfiguredEndpoint()
{
    using var document = JsonDocument.Parse(File.ReadAllText("Squirix.settings.json"));
    return document.RootElement.GetProperty("Squirix").GetProperty("Cluster").GetProperty("Url").GetString() ??
           throw new InvalidOperationException("Squirix.settings.json does not contain Squirix:Cluster:Url.");
}
