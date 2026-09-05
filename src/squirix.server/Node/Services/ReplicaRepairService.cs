using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Owns bounded follower-repair work for the host lifecycle.</summary>
internal sealed class ReplicaRepairService : BackgroundService
{
    private readonly ReplicaRepairPlanner _planner;
    private readonly Channel<RepairWork> _queue;
    private int _pendingCount;

    /// <summary>Initializes a new instance of the <see cref="ReplicaRepairService" /> class.</summary>
    /// <param name="capacity">Maximum queued repairs, excluding the active repair.</param>
    internal ReplicaRepairService(int capacity)
        : this(new ReplicaRepairPlanner(64), capacity)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaRepairService" /> class.</summary>
    /// <param name="planner">Bounded repair planner shared by sessions.</param>
    /// <param name="capacity">Maximum queued repairs, excluding the active repair.</param>
    internal ReplicaRepairService(ReplicaRepairPlanner planner, int capacity)
    {
        ArgumentNullException.ThrowIfNull(planner);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

        _planner = planner;
        Capacity = capacity;
        _queue = Channel.CreateBounded<RepairWork>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });
    }

    /// <summary>Gets the fixed queue capacity.</summary>
    internal int Capacity { get; }

    /// <summary>Gets queued and active work count.</summary>
    internal int PendingCount => Volatile.Read(ref _pendingCount);

    /// <inheritdoc />
    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _ = _queue.Writer.TryComplete();
        return base.StopAsync(cancellationToken);
    }

    /// <summary>Creates a catch-up session whose work can be submitted to this lifecycle-owned queue.</summary>
    /// <param name="follower">Target follower log.</param>
    /// <param name="eligibility">Replica-group participation gate.</param>
    /// <param name="replicaIndex">Target replica slot.</param>
    /// <returns>A snapshot catch-up session.</returns>
    internal ReplicaSnapshotCatchUpSession CreateSnapshotSession(IFollowerLog follower, ReplicaEligibility eligibility, int replicaIndex) =>
        new(_planner, follower, eligibility, replicaIndex);

    /// <summary>Attempts to enqueue one observable repair operation.</summary>
    /// <param name="repair">Repair callback.</param>
    /// <param name="cancellationToken">Per-operation cancellation token.</param>
    /// <param name="completion">Completion observed by the caller when accepted.</param>
    /// <returns><see langword="true" /> when accepted; otherwise <see langword="false" />.</returns>
    internal bool TryQueue(Func<CancellationToken, ValueTask> repair, CancellationToken cancellationToken, out Task completion)
    {
        ArgumentNullException.ThrowIfNull(repair);
        var work = new RepairWork(repair, cancellationToken);
        if (!_queue.Writer.TryWrite(work))
        {
            completion = Task.CompletedTask;
            return false;
        }

        _ = Interlocked.Increment(ref _pendingCount);
        completion = work.Completion;
        return true;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (await _queue.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
                while (_queue.Reader.TryRead(out var work))
                    await ExecuteWorkAsync(work, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown cancels the active operation. The finally block drains queued work as canceled.
        }
        finally
        {
            while (_queue.Reader.TryRead(out var work))
            {
                work.Cancel(stoppingToken);
                _ = Interlocked.Decrement(ref _pendingCount);
            }
        }
    }

    private async Task ExecuteWorkAsync(RepairWork work, CancellationToken stoppingToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, work.CancellationToken);
        try
        {
            await work.Callback(linked.Token).ConfigureAwait(false);
            work.Complete();
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested)
        {
            work.Cancel(linked.Token);
        }
        catch (Exception exception) when (exception is InvalidOperationException or TimeoutException or ObjectDisposedException or IOException
            or InvalidDataException or OperationCanceledException)
        {
            // Repair failures are delivered to the caller while the loop survives: storage, timeout,
            // disposal, and cancellation faults are all expected from follower repair work. Anything else
            // is a programming bug and fails fast rather than silently continuing on corrupt state.
            work.Fail(exception);
        }
        finally
        {
            _ = Interlocked.Decrement(ref _pendingCount);
        }
    }

    private sealed class RepairWork
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal RepairWork(Func<CancellationToken, ValueTask> callback, CancellationToken cancellationToken)
        {
            Callback = callback;
            CancellationToken = cancellationToken;
        }

        internal Func<CancellationToken, ValueTask> Callback { get; }

        internal CancellationToken CancellationToken { get; }

        internal Task Completion => _completion.Task;

        internal void Cancel(CancellationToken cancellationToken) => _ = _completion.TrySetCanceled(cancellationToken);

        internal void Complete() => _ = _completion.TrySetResult();

        internal void Fail(Exception exception) => _ = _completion.TrySetException(exception);
    }
}
