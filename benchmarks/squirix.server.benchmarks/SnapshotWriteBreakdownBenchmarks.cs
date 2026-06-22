using System;
using BenchmarkDotNet.Attributes;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates binary snapshot write costs: encode, temp-file write, and full publish.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class SnapshotWriteBreakdownBenchmarks
{
    private SnapshotWriteBreakdownSession? _session;
    private int _entryCount;
    private int _operationsPerInvoke;

    /// <summary>Full binary snapshot publish path (tmp write + rename).</summary>
    [Benchmark(Baseline = true)]
    public void PublishSnapshot()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            session.PublishSnapshot();
    }

    /// <summary>Encode all entry records into the reusable buffer (no I/O).</summary>
    [Benchmark]
    public void EncodeOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var total = 0;
        for (var i = 0; i < _operationsPerInvoke; i++)
            total += session.EncodeAllEntries();

        GC.KeepAlive(total);
    }

    /// <summary>Writes a complete temp snapshot file and flushes it to disk (no publish rename).</summary>
    [Benchmark]
    public void WriteTempFileOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        for (var i = 0; i < _operationsPerInvoke; i++)
            session.WriteTempFileOnly();
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
        _entryCount = SnapshotBenchmarkSupport.ResolveEntryCount();
        _session = SnapshotWriteBreakdownSession.Create(_entryCount);
    }
}
