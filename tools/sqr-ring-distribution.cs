#:project ../src/squirix.server/Squirix.Server.csproj
#:property PublishAot=false
using Squirix.Server.Cluster;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 0 || (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase) ||
                                              string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase) ||
                                              string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase))))
{
    await output.WriteLineAsync("sqr-ring-distribution — sample key ownership distribution in consistent hash ring.").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Usage:").ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/sqr-ring-distribution.cs -- --nodes node-a,node-b,node-c [--sample-size 10000] [--virtual-nodes 128] [--cache default]").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Exit codes: 0 ok, 2 usage, 3 internal").ConfigureAwait(false);
    return 0;
}

string? nodesCsv = null;
var cacheName = "default";
var sampleSize = 10000;
var virtualNodes = 128;
var argIndex = 0;
while (argIndex < argv.Length)
{
    var a = argv[argIndex];
    if (string.Equals(a, "--nodes", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length)
            return await Usage("missing value for --nodes").ConfigureAwait(false);

        nodesCsv = argv[argIndex + 1];
        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "--sample-size", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length || !int.TryParse(argv[argIndex + 1], out sampleSize) || sampleSize <= 0)
            return await Usage("invalid --sample-size value").ConfigureAwait(false);

        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "--virtual-nodes", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length || !int.TryParse(argv[argIndex + 1], out virtualNodes) || virtualNodes <= 0)
            return await Usage("invalid --virtual-nodes value").ConfigureAwait(false);

        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "--cache", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length)
            return await Usage("missing value for --cache").ConfigureAwait(false);

        cacheName = argv[argIndex + 1];
        argIndex += 2;
        continue;
    }

    return await Usage($"unknown argument '{a}'").ConfigureAwait(false);
}

if (string.IsNullOrWhiteSpace(nodesCsv))
    return await Usage("--nodes is required").ConfigureAwait(false);

try
{
    var nodes = nodesCsv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal).ToArray();
    if (nodes.Length == 0)
        return await Usage("--nodes must contain at least one node id").ConfigureAwait(false);

    var ring = new ConsistentHashRing(nodes, virtualNodes);
    var distribution = new Dictionary<string, int>(StringComparer.Ordinal);
    foreach (var node in nodes)
        distribution[node] = 0;

    for (var i = 0; i < sampleSize; i++)
    {
        var key = $"sample-key-{i}";
        var owner = ring.GetOwner(cacheName, key);
        distribution[owner] = distribution.TryGetValue(owner, out var count) ? count + 1 : 1;
    }

    await output.WriteLineAsync("OK: ring distribution computed").ConfigureAwait(false);
    await output.WriteLineAsync($"cache: {cacheName}").ConfigureAwait(false);
    await output.WriteLineAsync($"virtualNodes: {virtualNodes}").ConfigureAwait(false);
    await output.WriteLineAsync($"sampleSize: {sampleSize}").ConfigureAwait(false);
    foreach (var item in distribution.OrderBy(static x => x.Key, StringComparer.Ordinal))
    {
        var share = Math.Round((double)item.Value / sampleSize, 6);
        await output.WriteLineAsync($"node.{item.Key}.count: {item.Value}").ConfigureAwait(false);
        await output.WriteLineAsync($"node.{item.Key}.share: {share}").ConfigureAwait(false);
    }

    return 0;
}
catch (InvalidOperationException ex)
{
    await output.WriteLineAsync("ERROR: unexpected internal failure").ConfigureAwait(false);
    await output.WriteLineAsync(ex.Message).ConfigureAwait(false);
    return 3;
}
catch (ArgumentException ex)
{
    await output.WriteLineAsync("ERROR: unexpected internal failure").ConfigureAwait(false);
    await output.WriteLineAsync(ex.Message).ConfigureAwait(false);
    return 3;
}

static async Task<int> Usage(string message)
{
    await Console.Out.WriteLineAsync($"ERROR: {message}").ConfigureAwait(false);
    return 2;
}
