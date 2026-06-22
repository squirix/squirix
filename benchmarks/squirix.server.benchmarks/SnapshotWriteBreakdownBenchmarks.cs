using System;
using BenchmarkDotNet.Attributes;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates binary snapshot write costs: temp-file write and full publish.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class SnapshotWriteBreakdownBenchmarks
{
    private SnapshotWriteBreakdownSession? _session;
    private int _operationsPerInvoke;

    /// <summary>Full binary snapshot publish path (tmp write + rename).</summary>
    [Benchmark(Baseline = true)]
    public void PublishSnapshot()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            session.PublishSnapshot();
    }

    /// <summary>Writes a complete temp snapshot file and flushes it to disk (no publish rename).</summary>
    [Benchmark]
    public void WriteTempFileOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            session.WriteTempFileOnly();
    }

    /// <summary>Manifest store update after snapshot (encode + durable manifest file + pointer; no snapshot file I/O).</summary>
    [Benchmark]
    public void ManifestWriteOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            session.WriteManifestOnly();
    }

    /// <summary>Disposes the breakdown session and temporary data directory.</summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _session?.Dispose();
        _session = null;
    }

    /// <summary>Creates a warmed binary snapshot breakdown session.</summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        _operationsPerInvoke = SnapshotBenchmarkSupport.ResolveOperationsPerInvoke(2);
        _session = SnapshotWriteBreakdownSession.Create(SnapshotBenchmarkSupport.ResolveEntryCount());
    }
}
