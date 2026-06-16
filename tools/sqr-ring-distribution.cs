#:project ../src/squirix.server/Squirix.Server.csproj
#:property PublishAot=false
using System.Globalization;
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
            return await UsageAsync("missing value for --nodes").ConfigureAwait(false);

        nodesCsv = argv[argIndex + 1];
        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "--sample-size", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length || !int.TryParse(argv[argIndex + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out sampleSize) || sampleSize <= 0)
            return await UsageAsync("invalid --sample-size value").ConfigureAwait(false);

        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "--virtual-nodes", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length || !int.TryParse(argv[argIndex + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out virtualNodes) || virtualNodes <= 0)
            return await UsageAsync("invalid --virtual-nodes value").ConfigureAwait(false);

        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "--cache", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length)
            return await UsageAsync("missing value for --cache").ConfigureAwait(false);

        cacheName = argv[argIndex + 1];
        argIndex += 2;
        continue;
    }

    return await UsageAsync($"unknown argument '{a}'").ConfigureAwait(false);
}

if (string.IsNullOrWhiteSpace(nodesCsv))
    return await UsageAsync("--nodes is required").ConfigureAwait(false);

try
{
    var nodes = nodesCsv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal).ToArray();
    if (nodes.Length is 0)
        return await UsageAsync("--nodes must contain at least one node id").ConfigureAwait(false);

    var ring = new ConsistentHashRing(nodes, virtualNodes);
    var distribution = new Dictionary<string, int>(StringComparer.Ordinal);
    foreach (var node in nodes)
        distribution[node] = 0;

    for (var i = 0; i < sampleSize; i++)
    {
        var key = $"sample-key-{i.ToString(CultureInfo.InvariantCulture)}";
        var owner = ring.GetOwner(cacheName, key);
        distribution[owner] = distribution.TryGetValue(owner, out var count) ? count + 1 : 1;
    }

    await output.WriteLineAsync("OK: ring distribution computed").ConfigureAwait(false);
    await output.WriteLineAsync($"cache: {cacheName}").ConfigureAwait(false);
    await output.WriteLineAsync($"virtualNodes: {virtualNodes.ToString(CultureInfo.InvariantCulture)}").ConfigureAwait(false);
    await output.WriteLineAsync($"sampleSize: {sampleSize.ToString(CultureInfo.InvariantCulture)}").ConfigureAwait(false);
    foreach (var key in distribution.Keys.Order(StringComparer.Ordinal))
    {
        var count = distribution[key];
        var share = Math.Round(Convert.ToDouble(count, CultureInfo.InvariantCulture) / sampleSize, 6, MidpointRounding.ToEven);
        await output.WriteLineAsync($"node.{key}.count: {count.ToString(CultureInfo.InvariantCulture)}").ConfigureAwait(false);
        await output.WriteLineAsync($"node.{key}.share: {share.ToString(CultureInfo.InvariantCulture)}").ConfigureAwait(false);
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

static async Task<int> UsageAsync(string message)
{
    await Console.Out.WriteLineAsync($"ERROR: {message}").ConfigureAwait(false);
    return 2;
}
