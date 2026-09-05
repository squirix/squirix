using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Benchmarks;

/// <summary>Follower-repair planning cost without journal I/O.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class ReplicaRepairBenchmarks
{
    private readonly Consumer _consumer = new();
    private FollowerLogEntry[] _large = [];
    private ReplicaRepairPlanner _planner = new(64);
    private FollowerLogEntry[] _small = [];

    /// <summary>Selects a bounded repair batch backed up from a large divergent tail.</summary>
    [Benchmark]
    public void SelectBatchBackedUp()
    {
        var batch = _planner.SelectBatch(_large, 3_201UL);
        _consumer.Consume(batch.Entries.Length);
        _consumer.Consume(batch.PrevLogIndex);
    }

    /// <summary>Selects a bounded repair batch from a small sequential run.</summary>
    [Benchmark]
    public void SelectBatchSequential()
    {
        var batch = _planner.SelectBatch(_small, 2UL);
        _consumer.Consume(batch.Entries.Length);
        _consumer.Consume(batch.PrevLogIndex);
    }

    /// <summary>Sweeps the next-index backup computation.</summary>
    [Benchmark(OperationsPerInvoke = 1_024)]
    public void BackUpNextIndexSweep()
    {
        for (var i = 4_096UL; i < 5_120UL; i++)
            _consumer.Consume(ReplicaRepairPlanner.BackUpNextIndex(i, 64UL));
    }

    /// <summary>Builds the repair entry runs reused across invocations.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _planner = new ReplicaRepairPlanner(64);
        var payload = Encoding.UTF8.GetBytes("v");
        _small = new FollowerLogEntry[4];
        var index = 0;
        for (var i = 1UL; i <= 4UL; i++)
        {
            _small[index] = new FollowerLogEntry(i, 1UL, payload);
            index++;
        }

        _large = new FollowerLogEntry[4_096];
        index = 0;
        for (var i = 1UL; i <= 4_096UL; i++)
        {
            _large[index] = new FollowerLogEntry(i, i <= 2_048UL ? 1UL : 2UL, payload);
            index++;
        }
    }
}
