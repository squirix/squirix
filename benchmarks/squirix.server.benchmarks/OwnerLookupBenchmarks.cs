using System.Globalization;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures ownership lookup bursts with a different key on each invocation.</summary>
[MemoryDiagnoser]
[InProcess]
[WarmupCount(2)]
[IterationCount(5)]
public class OwnerLookupBenchmarks
{
    private readonly string[] _keys = new string[256];
    private INodeLocator? _locator;
    private int _next;

    /// <summary>Gets or sets the key length.</summary>
    [Params(16, 1024)]
    public int KeyLength { get; set; }

    /// <summary>Gets or sets the number of layers resolving the same key.</summary>
    [Params(1, 4, 7)]
    public int Lookups { get; set; }

    /// <summary>Builds the ring and distinct request keys.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _locator = RuntimeServiceRegistration.CreateHashLocator(["n1", "n2", "n3"]);
        for (var i = 0; i < _keys.Length; i++)
            _keys[i] = i.ToString("D4", CultureInfo.InvariantCulture) + new string('x', KeyLength - 4);
    }

    /// <summary>Resolves one request through several synchronous layers.</summary>
    /// <returns>The selected owner.</returns>
    [Benchmark]
    public string Resolve()
    {
        var locator = ThrowHelper.Required(_locator, "Benchmark was not initialized.");
        var key = _keys[_next++ & 255];
        var owner = string.Empty;
        for (var i = 0; i < Lookups; i++)
            owner = locator.GetOwner("cache", key);

        return owner;
    }
}
