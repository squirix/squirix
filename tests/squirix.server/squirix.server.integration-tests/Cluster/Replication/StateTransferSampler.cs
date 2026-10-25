using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Samples process memory, the estimated cache bytes of every node, the disk use of one node and the lag of one follower while a measurement runs.</summary>
/// <remarks>
/// All nodes of the cluster share one process, so the process figures (working set, managed heap) cover every node and the test itself; the per-node
/// figures are the estimated cache bytes each node accounts for, which is what its memory pressure state reads.
/// </remarks>
internal sealed class StateTransferSampler : IAsyncDisposable
{
    private static readonly TimeSpan Period = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan DiskPeriod = TimeSpan.FromSeconds(1);

    private readonly CancellationTokenSource _cancel = new();
    private readonly Func<string, IMemoryUsageAccounting?> _accounting;
    private readonly string[] _nodes;
    private readonly string _watchedDirectory;
    private readonly Func<CancellationToken, ValueTask<long>> _lag;
    private readonly Func<CancellationToken, ValueTask<string>> _trace;
    private readonly List<string> _timeline = [];
    private readonly Lock _gate = new();
    private Task _loop = Task.CompletedTask;
    private Peaks _peaks;

    /// <summary>Initializes a new instance of the <see cref="StateTransferSampler" /> class.</summary>
    /// <param name="nodes">The node identifiers whose estimated cache bytes are sampled.</param>
    /// <param name="accounting">Resolves the memory accounting of a node; <see langword="null" /> while the node is down.</param>
    /// <param name="watchedDirectory">The data directory whose size is sampled.</param>
    /// <param name="lag">Reads how many entries the watched follower is behind the leader.</param>
    /// <param name="trace">Describes the replication state once a second for the timeline.</param>
    internal StateTransferSampler(string[] nodes, Func<string, IMemoryUsageAccounting?> accounting, string watchedDirectory, Func<CancellationToken, ValueTask<long>> lag, Func<CancellationToken, ValueTask<string>> trace)
    {
        _trace = trace;
        _nodes = nodes;
        _accounting = accounting;
        _watchedDirectory = watchedDirectory;
        _lag = lag;
        _peaks = new Peaks(0, 0, 0, new long[nodes.Length], 0, 0, 0);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _cancel.CancelAsync();
        await _loop;
        _cancel.Dispose();
    }

    /// <summary>Sums the sizes of the files under a directory.</summary>
    /// <param name="directory">The directory.</param>
    /// <returns>The total length in bytes; zero when the directory does not exist.</returns>
    internal static long DirectoryBytes(string directory)
    {
        if (!Directory.Exists(directory))
            return 0;

        long total = 0;
        foreach (var file in new DirectoryInfo(directory).EnumerateFiles("*", new EnumerationOptions { RecurseSubdirectories = true, IgnoreInaccessible = true }))
        {
            try
            {
                total += file.Length;
            }
            catch (FileNotFoundException)
            {
                // A segment deleted while the walk runs no longer counts.
            }
        }

        return total;
    }

    /// <summary>Starts sampling.</summary>
    internal void Start() => _loop = RunAsync(_cancel.Token);

    /// <summary>Returns the timeline lines recorded since the last call.</summary>
    /// <returns>One line per second.</returns>
    internal string[] TakeTimeline()
    {
        lock (_gate)
        {
            string[] lines = [.. _timeline];
            _timeline.Clear();
            return lines;
        }
    }

    /// <summary>Returns the highest values seen since the last call and starts a new period.</summary>
    /// <returns>The peaks.</returns>
    internal Peaks TakePeaks()
    {
        lock (_gate)
        {
            var taken = _peaks;
            _peaks = new Peaks(0, 0, 0, new long[_nodes.Length], 0, 0, 0);
            return taken;
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        await Task.Yield();
        using var timer = new PeriodicTimer(Period);
        var lastDisk = DateTime.MinValue;
        var lastTick = Stopwatch.GetTimestamp();
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                var gap = long.CreateChecked(Stopwatch.GetElapsedTime(lastTick).TotalMilliseconds);
                lastTick = Stopwatch.GetTimestamp();
                var nodeBytes = new long[_nodes.Length];
                for (var i = 0; i < _nodes.Length; i++)
                    nodeBytes[i] = _accounting(_nodes[i])?.ReadEstimatedBytes() ?? 0;

                var disk = 0L;
                if (DateTime.UtcNow - lastDisk >= DiskPeriod)
                {
                    lastDisk = DateTime.UtcNow;
                    disk = DirectoryBytes(_watchedDirectory);
                    var line = await _trace(cancellationToken);
                    lock (_gate)
                        _timeline.Add(line);
                }

                var lag = await _lag(cancellationToken);
                lock (_gate)
                {
                    var current = _peaks;
                    var perNode = current.NodeEstimatedBytes;
                    for (var i = 0; i < perNode.Length; i++)
                        perNode[i] = Math.Max(perNode[i], nodeBytes[i]);

                    _peaks = current with
                    {
                        WorkingSetBytes = Math.Max(current.WorkingSetBytes, Environment.WorkingSet),
                        ManagedBytes = Math.Max(current.ManagedBytes, GC.GetTotalMemory(false)),
                        CommittedBytes = Math.Max(current.CommittedBytes, GC.GetGCMemoryInfo().TotalCommittedBytes),
                        WatchedDiskBytes = Math.Max(current.WatchedDiskBytes, disk),
                        MaxLagEntries = Math.Max(current.MaxLagEntries, lag),
                        MaxTickGapMs = Math.Max(current.MaxTickGapMs, gap),
                    };
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The measurement ended.
        }
    }

    /// <summary>The highest values of one sampling period.</summary>
    /// <param name="WorkingSetBytes">The process working set.</param>
    /// <param name="ManagedBytes">The managed heap size including garbage not yet collected.</param>
    /// <param name="CommittedBytes">The memory the garbage collector had committed at its last collection.</param>
    /// <param name="NodeEstimatedBytes">The estimated cache bytes of each node, in the order of the node identifiers.</param>
    /// <param name="WatchedDiskBytes">The size of the watched data directory, sampled once a second.</param>
    /// <param name="MaxLagEntries">The largest number of entries the watched follower was behind.</param>
    /// <param name="MaxTickGapMs">The longest time between two sampler ticks, which a pause of the whole process stretches well past the period.</param>
    internal readonly record struct Peaks(long WorkingSetBytes, long ManagedBytes, long CommittedBytes, long[] NodeEstimatedBytes, long WatchedDiskBytes, long MaxLagEntries, long MaxTickGapMs);
}
