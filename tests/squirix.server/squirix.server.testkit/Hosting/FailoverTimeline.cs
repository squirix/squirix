using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Samples the leader tables of every running node of a test cluster every ten milliseconds in the background, and records when each
/// <see cref="FailoverPhase" /> of one replica group was first seen, with the events the test marks.
/// </summary>
/// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
/// <remarks>
/// The phases are measured against the leader that held authority when the timeline started; when none held it, the group is taken to
/// have lost its leader from the start. Times are read from the system monotonic clock. <see cref="Dump" /> renders the timeline for an
/// assertion message. Nodes that stop while a sample reads them are skipped for that sample.
/// </remarks>
[ThreadSafe]
internal sealed class FailoverTimeline<TOptions> : IAsyncDisposable
    where TOptions : ClusterStartOptions
{
    private readonly TestCluster<TOptions> _cluster;
    private readonly List<(TimeSpan At, string What)> _events = [];
    private readonly Lock _gate = new();
    private readonly string _groupId;
    private readonly Dictionary<FailoverPhase, TimeSpan> _phases = [];
    private readonly Task _sampling;
    private readonly long _started;
    private readonly CancellationTokenSource _stop = new();
    private LeaderRoute _newLeader;
    private int _samples;

    private FailoverTimeline(TestCluster<TOptions> cluster, string groupId)
    {
        _cluster = cluster;
        _groupId = groupId;
        _started = TimeProvider.System.GetTimestamp();
        Baseline = AuthorityHolder();
        _sampling = RunAsync(_stop.Token);
    }

    /// <summary>Gets the leader that held authority when the timeline started; <see langword="default" /> when none did.</summary>
    internal LeaderRoute Baseline { get; }

    /// <summary>Gets the first leader seen in a term above the baseline; <see langword="default" /> until one is seen.</summary>
    internal LeaderRoute NewLeader
    {
        get
        {
            lock (_gate)
                return _newLeader;
        }
    }

    /// <summary>Gets when a phase was first seen, measured from the start of the timeline; <see langword="null" /> until it is.</summary>
    /// <param name="phase">The phase.</param>
    /// <returns>The elapsed time at the first sample that saw the phase.</returns>
    internal TimeSpan? this[FailoverPhase phase]
    {
        get
        {
            lock (_gate)
                return _phases.TryGetValue(phase, out var at) ? at : null;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);

#pragma warning disable VSTHRD003 // The sampling loop is started by this timeline's constructor and ends once the stop token above is cancelled.
        await _sampling.ConfigureAwait(false);
#pragma warning restore VSTHRD003

        _stop.Dispose();
    }

    /// <summary>Starts sampling the leader tables of a cluster for one group.</summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="groupId">The replica group.</param>
    /// <returns>The running timeline; dispose it to stop sampling.</returns>
    internal static FailoverTimeline<TOptions> Start(TestCluster<TOptions> cluster, string groupId)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        return new FailoverTimeline<TOptions>(cluster, groupId);
    }

    /// <summary>Renders the timeline, one line per phase and event in time order, for an assertion message.</summary>
    /// <returns>The rendered timeline.</returns>
    internal string Dump()
    {
        List<(TimeSpan At, string What)> lines;
        int samples;
        lock (_gate)
        {
            lines = [with(_events.Count + _phases.Count), .. _events];
            foreach (var (phase, at) in _phases)
                lines.Add((at, Name(phase)));

            samples = _samples;
        }

        lines.Sort(static (a, b) => a.At.CompareTo(b.At));
        var text = new StringBuilder();
        _ = text.Append(CultureInfo.InvariantCulture, $"Failover timeline of group {_groupId}, started under leader '{Baseline.NodeId}' in term {Baseline.Term}, {samples} samples:");
        foreach (var (at, what) in lines)
            _ = text.AppendLine().Append(CultureInfo.InvariantCulture, $"  +{at.TotalMilliseconds,8:F0} ms  {what}");

        return text.ToString();
    }

    /// <summary>Records an event of the test, such as the start of a fault, at the current time.</summary>
    /// <param name="what">What happened.</param>
    internal void Mark(string what)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(what);
        var at = TimeProvider.System.GetElapsedTime(_started);
        lock (_gate)
            _events.Add((at, what));
    }

    private static string Name(FailoverPhase phase) => phase switch
    {
        FailoverPhase.LeaderLost => nameof(FailoverPhase.LeaderLost),
        FailoverPhase.TermRaised => nameof(FailoverPhase.TermRaised),
        FailoverPhase.NewLeader => nameof(FailoverPhase.NewLeader),
        FailoverPhase.Converged => nameof(FailoverPhase.Converged),
        _ => throw new ArgumentOutOfRangeException(nameof(phase), phase, "Unsupported enum value."),
    };

    private LeaderRoute AuthorityHolder()
    {
        var holder = default(LeaderRoute);
        for (var i = 0; i < _cluster.Topology.Count; i++)
        {
            if (TryRead(_cluster.Topology[i].NodeId, out var view) && view.HasAuthority && view.Term > holder.Term)
                holder = new LeaderRoute(_cluster.Topology[i].NodeId, view.Term);
        }

        return holder;
    }

    private bool Converged(LeaderRoute leader)
    {
        for (var i = 0; i < _cluster.Topology.Count; i++)
        {
            if (TryRead(_cluster.Topology[i].NodeId, out var view) && (view.Term != leader.Term || view.Known != leader))
                return false;
        }

        return true;
    }

    private void Record(FailoverPhase phase, TimeSpan at) => _ = _phases.TryAdd(phase, at);

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10), TimeProvider.System);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                Sample();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The timeline was disposed: sampling ends.
        }
    }

    private void Sample()
    {
        var at = TimeProvider.System.GetElapsedTime(_started);
        var baselineHeld = false;
        var raised = false;
        var holder = AuthorityHolder();
        for (var i = 0; i < _cluster.Topology.Count; i++)
        {
            if (!TryRead(_cluster.Topology[i].NodeId, out var view))
                continue;

            baselineHeld |= Baseline.Term != 0UL && view.HasAuthority && view.Known == Baseline;
            raised |= view.HighestObservedTerm > Baseline.Term;
        }

        var converged = holder.Term > Baseline.Term && Converged(holder);
        lock (_gate)
        {
            _samples++;
            if (!baselineHeld)
                Record(FailoverPhase.LeaderLost, at);

            if (raised)
                Record(FailoverPhase.TermRaised, at);

            if (holder.Term > Baseline.Term && _newLeader.Term == 0UL)
            {
                _newLeader = holder;
                Record(FailoverPhase.NewLeader, at);
            }

            if (converged)
                Record(FailoverPhase.Converged, at);
        }
    }

    /// <summary>Reads what a running node knows of the leader of the group.</summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="view">The view; <see langword="default" /> when the node does not run, is stopping, or does not serve the group.</param>
    /// <returns><see langword="true" /> when the node runs and serves the group.</returns>
    private bool TryRead(string nodeId, out GroupLeaderView view)
    {
        view = default;
        if (!_cluster.TryGetNode(nodeId, out var node))
            return false;

        try
        {
            view = node.GetRequiredService<IGroupLeaderTable>().Read(_groupId);
        }
        catch (ObjectDisposedException)
        {
            // The node is stopping: its services are disposed while this sample reads it.
            return false;
        }

        return view.Served;
    }
}
