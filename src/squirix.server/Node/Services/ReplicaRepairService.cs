using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>Owns bounded follower-repair work for the host lifecycle.</summary>
internal sealed class ReplicaRepairService : BackgroundService
{
    private readonly Channel<RepairWork> _queue;
    private int _pendingCount;

    /// <summary>Initializes a new instance of the <see cref="ReplicaRepairService" /> class.</summary>
    /// <param name="capacity">Maximum queued repairs, excluding the active repair.</param>
    internal ReplicaRepairService(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

        Capacity = capacity;
        var boundedChannelOptions = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        };
        _queue = Channel.CreateBounded<RepairWork>(boundedChannelOptions);
    }

    /// <summary>Gets the fixed queue capacity.</summary>
    internal int Capacity { get; }

    /// <summary>Gets queued and active work count.</summary>
    internal int PendingCount => Volatile.Read(ref _pendingCount);

    /// <summary>Gets the queue completion task that completes when the worker terminates. Test seam.</summary>
    internal Task ReaderCompletion => _queue.Reader.Completion;

    /// <inheritdoc />
    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _ = _queue.Writer.TryComplete();
        return base.StopAsync(cancellationToken);
    }

    /// <summary>Attempts to enqueue one observable repair operation.</summary>
    /// <param name="repair">Repair callback.</param>
    /// <param name="cancellationToken">Per-operation cancellation token.</param>
    /// <param name="completion">Completion observed by the caller when accepted.</param>
    /// <returns><see langword="true" /> when accepted; otherwise <see langword="false" />.</returns>
    internal bool TryQueue(Func<CancellationToken, ValueTask> repair, CancellationToken cancellationToken, out Task completion)
    {
        ArgumentNullException.ThrowIfNull(repair);
        var work = new RepairWork(repair, cancellationToken);

        // Reserve the count before publishing: a synchronously draining reader must never observe
        // the item without its reservation.
        ReserveSlot();
        if (!_queue.Writer.TryWrite(work))
        {
            ReleaseSlot();
            completion = Task.CompletedTask;
            return false;
        }

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
        catch (Exception)
        {
            // Unexpected worker death: complete the writer before draining, so TryQueue rejects
            // late arrivals instead of accepting repair work no reader will ever run.
            _ = _queue.Writer.TryComplete();
            throw;
        }
        finally
        {
            while (_queue.Reader.TryRead(out var work))
            {
                work.Cancel(stoppingToken);
                ReleaseSlot();
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
        catch (Exception exception) when (exception is InvalidOperationException or TimeoutException or ObjectDisposedException or IOException or InvalidDataException
                                               or OperationCanceledException)
        {
            // Repair failures are delivered to the caller while the loop survives: storage, timeout,
            // disposal, and cancellation faults are all expected from follower repair work. An
            // OperationCanceledException outside the linked scope means foreign cancellation, which is
            // still a per-operation failure rather than a loop defect. Anything else is a programming
            // bug and fails fast rather than silently continuing in a corrupt state.
            work.Fail(exception);
        }
        catch (Exception exception)
        {
            // Unexpected callback faults fail the caller's completion before the loop fails fast, so the
            // TryQueue task completes faulted instead of hanging while the host shuts down.
            work.Fail(exception);
            throw;
        }
        finally
        {
            ReleaseSlot();
        }
    }

    private void ReleaseSlot() => _ = Interlocked.Decrement(ref _pendingCount);

    private void ReserveSlot() => _ = Interlocked.Increment(ref _pendingCount);

    [Immutable]
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
