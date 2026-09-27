using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Journal event loop host double over a caller-owned pending-append registry: completes durability checkpoints, optionally decrements a
/// queued-append counter, records the last pipeline failure and runs a test callback on roll publication.
/// </summary>
[Mutable]
internal sealed class FakeEventLoopHost : IJournalEventLoopHost
{
    private readonly PendingAppendRegistry _pendingAppends;
    private readonly MutableInt32? _queuedAppends;

    /// <summary>Initializes a new instance of the <see cref="FakeEventLoopHost" /> class.</summary>
    /// <param name="pendingAppends">The pending-append registry the journal thread tracks appends in.</param>
    /// <param name="queuedAppends">The queued-append counter the journal thread decrements, or <see langword="null" /> to ignore decrements.</param>
    internal FakeEventLoopHost(PendingAppendRegistry pendingAppends, MutableInt32? queuedAppends = null)
    {
        _pendingAppends = pendingAppends;
        _queuedAppends = queuedAppends;
    }

    PendingAppendRegistry IJournalEventLoopHost.PendingAppends => _pendingAppends;

    /// <summary>Gets the reason of the last pipeline failure the journal thread reported, if any.</summary>
    internal Exception? PipelineFailure { get; private set; }

    /// <summary>Gets or sets the callback run when the journal thread publishes a roll, standing in for the manifest roll thread.</summary>
    internal Action? RollPublished { get; set; }

    void IJournalEventLoopHost.CompleteDurabilityCheckpoint(JournalWorkItem item) => _ = item.Ack?.TrySetResult();

    void IJournalEventLoopHost.DecrementQueuedAppends()
    {
        if (_queuedAppends != null)
            _ = Interlocked.Decrement(ref _queuedAppends.Value);
    }

    void IJournalEventLoopHost.FailPipeline(Exception reason) => PipelineFailure = reason;

    void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex) => RollPublished?.Invoke();

    void IJournalEventLoopHost.SetNextSequence(ulong value)
    {
    }

    void IJournalEventLoopHost.ThrowIfJournalThreadFailed()
    {
    }
}
