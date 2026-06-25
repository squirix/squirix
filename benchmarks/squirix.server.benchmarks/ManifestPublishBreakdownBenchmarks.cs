using System;
using BenchmarkDotNet.Attributes;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates segment-roll manifest costs: data-file fsync, pointer fsync, and full publish.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public sealed class ManifestPublishBreakdownBenchmarks
{
    private ManifestRollBreakdownSession? _session;
    private int _nextFileIndex = 10_000;
    private int _nextJournal = 2;
    private int _operationsPerInvoke;
    private ulong _nextSequence = 2;

    /// <summary>Full production roll publish path via manifest store roll blocking API.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark(Baseline = true)]
    public void PublishRollBlocking()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
            session.Store.PublishRollBlocking(_nextJournal++, _nextSequence++);
    }

    /// <summary>Creates a new <c>.bmqx</c> file and fsyncs it using a fixed pre-encoded roll payload.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public void RollDataFileOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var encodedLength = session.EncodeRoll(1, 1);
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
        {
            var path = session.BuildManifestFilePath(_nextFileIndex++);
            session.WriteDataFile(path, encodedLength);
        }
    }

    /// <summary>Overwrites <c>man-current</c> and fsyncs the pointer (no numbered manifest file).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public void RollPointerOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
            session.WritePointer(_nextFileIndex++);
    }

    /// <summary>Roll encode plus numbered manifest file write (no pointer update).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public void RollEncodeAndDataFile()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
        {
            var encodedLength = session.EncodeRoll(_nextJournal++, _nextSequence++);
            var path = session.BuildManifestFilePath(_nextFileIndex++);
            session.WriteDataFile(path, encodedLength);
        }
    }

    /// <summary>Disposes the breakdown session and temp data directory.</summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _session?.Dispose();
        _session = null;
    }

    /// <summary>Creates a warmed manifest session for the current parameter set.</summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        _operationsPerInvoke = ManifestBenchmarkSupport.ResolvePublishOperationsPerInvoke();
        _session = ManifestRollBreakdownSession.Create();
        _nextFileIndex = 10_000;
        _nextJournal = 2;
        _nextSequence = 2;
    }
}
