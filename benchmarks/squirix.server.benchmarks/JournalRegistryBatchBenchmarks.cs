using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures tracking, staging checks, batch drain checks and completion ownership without segment I/O.</summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class JournalRegistryBatchBenchmarks
{
    private const int Operations = 65536;
    private readonly List<PendingAppendEntry> _completed = [];
    private readonly PendingAppendRegistry _registry = new();
    private JournalWorkItem[] _items = [];

    /// <summary>Gets or sets the number of frames completed together.</summary>
    [Params(1, 32, 256)]
    public int BatchSize { get; set; }

    /// <summary>Gets or sets a value indicating whether completion claims the batch together or calls the original per-item API.</summary>
    [Params(false, true)]
    public bool BatchRemoval { get; set; }

    /// <summary>Creates fixed items and preallocates completion storage.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _items = new JournalWorkItem[BatchSize];
        for (var i = 0; i < _items.Length; i++)
            _items[i] = JournalWorkItem.Append(new byte[64], 64);
        _completed.Capacity = BatchSize;
    }

    /// <summary>Runs an identical registry lifecycle for each frame, including the two checks around a batch write.</summary>
    /// <exception cref="InvalidOperationException">The lifecycle lost a tracked item.</exception>
    [Benchmark(OperationsPerInvoke = Operations)]
    public void TrackCheckComplete()
    {
        for (var round = 0; round < Operations / BatchSize; round++)
        {
            for (var i = 0; i < _items.Length; i++)
            {
                var item = _items[i];
                _registry.Track(item, item.FrameBytes ?? [], item.FrameLength, null);
                if (_registry.IsAbandoned(item))
                    throw new InvalidOperationException("A tracked item was abandoned.");
            }

            if (_registry.AnyAbandoned(_items))
                throw new InvalidOperationException("A batch was abandoned before writing.");
            if (_registry.AnyAbandoned(_items))
                throw new InvalidOperationException("A batch was abandoned after writing.");
            _completed.Clear();
            CompleteTrackedBatch();
            if (_completed.Count != BatchSize || _registry.PendingCount != 0)
                throw new InvalidOperationException("Completion lost a tracked frame.");
        }
    }

    private static void CompleteIndividually(PendingAppendRegistry registry, JournalWorkItem[] items, List<PendingAppendEntry> completed)
    {
        for (var i = 0; i < items.Length; i++)
        {
            if (registry.Untrack(items[i], out var entry) && entry != null)
                completed.Add(entry);
        }
    }

    private void CompleteTrackedBatch()
    {
        if (BatchRemoval)
            _registry.UntrackBatch(_items, _completed);
        else
            CompleteIndividually(_registry, _items, _completed);
    }
}
