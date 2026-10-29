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
    private NodeCacheEntry<object?> _entry = new(null, 1);
    private byte[] _previous = [];

    /// <summary>Gets or sets the previous payload size in bytes.</summary>
    [Params(0, 256, 65536)]
    public int PayloadBytes { get; set; }

    /// <summary>Encodes the applied flag and an already serialized previous payload.</summary>
    /// <returns>The owned outcome bytes.</returns>
    [Benchmark]
    public byte[] Encode() => ReplicaOutcomeCodec.Encode(true, _previous);

    /// <summary>Prepares a live remove, including protobuf serialization of the previous entry.</summary>
    /// <returns>The owned outcome bytes.</returns>
    [Benchmark]
    public byte[] Remove() => ReplicaMutationDecisions.DecideRemove(_entry, _now).Outcome;

    /// <summary>Creates the previous payload and the live entry outside the measurement.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _previous = new byte[PayloadBytes];
        _entry = new NodeCacheEntry<object?>(new string('x', PayloadBytes), 1);
    }
}
