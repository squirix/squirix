using System;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures canonical outcome encoding and live remove preparation at different payload sizes.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class ReplicaOutcomeBenchmarks
{
    private readonly DateTime _now = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private NodeCacheEntry<StoredValue> _entry = new(default);

    /// <summary>Gets or sets the previous payload size in bytes.</summary>
    [Params(0, 65536, 4190208)]
    public int PayloadBytes { get; set; }

    /// <summary>Prepares a live remove without serializing or retaining the previous value.</summary>
    /// <returns>The owned outcome bytes.</returns>
    [Benchmark]
    public ReadOnlyMemory<byte> Remove() => ReplicaMutationDecisions.DecideRemove(_entry, _now).Outcome;

    /// <summary>Creates the previous payload and the live entry outside the measurement.</summary>
    [GlobalSetup]
    public void Setup() => _entry = new NodeCacheEntry<StoredValue>(StoredValue.FromBinary(new byte[PayloadBytes]));
}
