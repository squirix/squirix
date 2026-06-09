#:project ../src/squirix.server/Squirix.Server.csproj
#:property PublishAot=false
using Squirix.Server.Cluster;

var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 0 || (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase) ||
                                              string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase) ||
                                              string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase))))
{
    Console.WriteLine("sqr-ring-distribution — sample key ownership distribution in consistent hash ring.");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run --file tools/sqr-ring-distribution.cs -- --nodes node-a,node-b,node-c [--sample-size 10000] [--virtual-nodes 128] [--cache default]");
    Console.WriteLine();
    Console.WriteLine("Exit codes: 0 ok, 2 usage, 3 internal");
    return 0;
}

string? nodesCsv = null;
var cacheName = "default";
var sampleSize = 10000;
var virtualNodes = 128;
for (var i = 0; i < argv.Length; i++)
{
    var a = argv[i];
    if (string.Equals(a, "--nodes", StringComparison.OrdinalIgnoreCase))
    {
        if (i + 1 >= argv.Length)
            return Usage("missing value for --nodes");

        nodesCsv = argv[++i];
        continue;
    }

    if (string.Equals(a, "--sample-size", StringComparison.OrdinalIgnoreCase))
    {
        if (i + 1 >= argv.Length || !int.TryParse(argv[++i], out sampleSize) || sampleSize <= 0)
            return Usage("invalid --sample-size value");

        continue;
    }

    if (string.Equals(a, "--virtual-nodes", StringComparison.OrdinalIgnoreCase))
    {
        if (i + 1 >= argv.Length || !int.TryParse(argv[++i], out virtualNodes) || virtualNodes <= 0)
            return Usage("invalid --virtual-nodes value");

        continue;
    }

    if (string.Equals(a, "--cache", StringComparison.OrdinalIgnoreCase))
    {
        if (i + 1 >= argv.Length)
            return Usage("missing value for --cache");

        cacheName = argv[++i];
        continue;
    }

    return Usage($"unknown argument '{a}'");
}

if (string.IsNullOrWhiteSpace(nodesCsv))
    return Usage("--nodes is required");

try
{
    var nodes = nodesCsv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal).ToArray();
    if (nodes.Length == 0)
        return Usage("--nodes must contain at least one node id");

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

    Console.WriteLine("OK: ring distribution computed");
    Console.WriteLine($"cache: {cacheName}");
    Console.WriteLine($"virtualNodes: {virtualNodes}");
    Console.WriteLine($"sampleSize: {sampleSize}");
    foreach (var item in distribution.OrderBy(static x => x.Key, StringComparer.Ordinal))
    {
        var share = Math.Round((double)item.Value / sampleSize, 6);
        Console.WriteLine($"node.{item.Key}.count: {item.Value}");
        Console.WriteLine($"node.{item.Key}.share: {share}");
    }

    return 0;
}
catch (InvalidOperationException ex)
{
    Console.WriteLine("ERROR: unexpected internal failure");
    Console.WriteLine(ex.Message);
    return 3;
}
catch (ArgumentException ex)
{
    Console.WriteLine("ERROR: unexpected internal failure");
    Console.WriteLine(ex.Message);
    return 3;
}

static int Usage(string message)
{
    Console.WriteLine($"ERROR: {message}");
    return 2;
}
