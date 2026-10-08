using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
/// assertion message. Nodes that stop while a sample reads them are skipped for that sample. A sample that fails ends the sampling; the
/// failure is kept in <see cref="Fault" />, and the phase indexer throws it, so a wait that polls a phase fails fast.
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
    private readonly LeaderRoute _baseline;
    private int _disposed;
    private Exception? _fault;
    private LeaderRoute _newLeader;
    private int _samples;

    private FailoverTimeline(TestCluster<TOptions> cluster, string groupId)
    {
        _cluster = cluster;
        _groupId = groupId;
        _started = TimeProvider.System.GetTimestamp();
        _baseline = AuthorityHolder();
        _sampling = RunAsync(_stop.Token);
    }

    /// <summary>Gets the leader that held authority when the timeline started, and its term; an empty identifier and term zero when none did.</summary>
    internal (string NodeId, ulong Term) Baseline => (_baseline.NodeId ?? string.Empty, _baseline.Term);

    /// <summary>Gets the failure that ended the sampling; <see langword="null" /> while sampling runs or after it ended by disposal.</summary>
    internal Exception? Fault
    {
        get
        {
            lock (_gate)
                return _fault;
        }
    }

    /// <summary>Gets the first leader seen in a term above the baseline, and its term; an empty identifier and term zero until one is seen.</summary>
    internal (string NodeId, ulong Term) NewLeader
    {
        get
        {
            lock (_gate)
                return (_newLeader.NodeId ?? string.Empty, _newLeader.Term);
        }
    }

    /// <summary>Gets when a phase was first seen, measured from the start of the timeline; <see langword="null" /> until it is.</summary>
    /// <param name="phase">The phase.</param>
    /// <returns>The elapsed time at the first sample that saw the phase.</returns>
    /// <exception cref="InvalidOperationException">A sample failed and ended the sampling; the failure is the inner exception.</exception>
    internal TimeSpan? this[FailoverPhase phase]
    {
        get
        {
            lock (_gate)
            {
                ThrowIfFaulted();
                return _phases.TryGetValue(phase, out var at) ? at : null;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        try
        {
            await _stop.CancelAsync().ConfigureAwait(false);

#pragma warning disable VSTHRD003 // The sampling loop is started by this timeline's constructor and ends once the stop token above is cancelled.
            await _sampling.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        }
        finally
        {
            _stop.Dispose();
        }
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

    /// <summary>Renders the timeline, one line per phase and event in time order, and the failure that ended the sampling, if any.</summary>
    /// <returns>The rendered timeline.</returns>
    internal string Dump()
    {
        List<(TimeSpan At, string What)> lines;
        int samples;
        Exception? fault;
        lock (_gate)
        {
            lines = [with(_events.Count + _phases.Count), .. _events];
            foreach (var (phase, at) in _phases)
                lines.Add((at, Name(phase)));

            samples = _samples;
            fault = _fault;
        }

        lines.Sort(static (a, b) => a.At.CompareTo(b.At));
        var text = new StringBuilder();
        _ = text.Append(CultureInfo.InvariantCulture, $"Failover timeline of group {_groupId}, started under leader '{_baseline.NodeId}' in term {_baseline.Term}, {samples} samples:");
        foreach (var (at, what) in lines)
            _ = text.AppendLine().Append(CultureInfo.InvariantCulture, $"  +{at.TotalMilliseconds,8:F0} ms  {what}");

        if (fault != null)
            _ = text.AppendLine().Append(CultureInfo.InvariantCulture, $"  sampling failed: {fault}");

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
            if (LeaderTableReads.TryRead(_cluster, _cluster.Topology[i].NodeId, _groupId, out var view) && view.HasAuthority && view.Term > holder.Term)
                holder = new LeaderRoute(_cluster.Topology[i].NodeId, view.Term);
        }

        return holder;
    }

    private bool Converged(LeaderRoute leader)
    {
        for (var i = 0; i < _cluster.Topology.Count; i++)
        {
            if (LeaderTableReads.TryRead(_cluster, _cluster.Topology[i].NodeId, _groupId, out var view) && (view.Term != leader.Term || view.Known != leader))
                return false;
        }

        return true;
    }

    private void Record(FailoverPhase phase, TimeSpan at) => _ = _phases.TryAdd(phase, at);

    /// <summary>Throws the failure that ended the sampling; called under the gate.</summary>
    /// <exception cref="InvalidOperationException">A sample failed; the failure is the inner exception.</exception>
    private void ThrowIfFaulted()
    {
        if (_fault != null)
            throw new InvalidOperationException($"The failover timeline of group {_groupId} stopped sampling.", _fault);
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "A failed sample must not vanish in an unobserved task: it is kept and surfaced through the phase indexer and the dump.")]
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
        catch (Exception exception)
        {
            lock (_gate)
                _fault = exception;
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
            if (!LeaderTableReads.TryRead(_cluster, _cluster.Topology[i].NodeId, _groupId, out var view))
                continue;

            baselineHeld |= _baseline.Term != 0UL && view.HasAuthority && view.Known == _baseline;
            raised |= view.HighestObservedTerm > _baseline.Term;
        }

        var converged = holder.Term > _baseline.Term && Converged(holder);
        lock (_gate)
        {
            _samples++;
            if (!baselineHeld)
                Record(FailoverPhase.LeaderLost, at);

            if (raised)
                Record(FailoverPhase.TermRaised, at);

            if (holder.Term > _baseline.Term && _newLeader.Term == 0UL)
            {
                _newLeader = holder;
                Record(FailoverPhase.NewLeader, at);
            }

            if (converged)
                Record(FailoverPhase.Converged, at);
        }
    }
}
