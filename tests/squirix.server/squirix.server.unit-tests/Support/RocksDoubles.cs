using System;
using System.Threading;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Rocks-backed factories for trivial shared test doubles (no delegate allocations).</summary>
internal static class RocksDoubles
{
    /// <summary>Creates an <see cref="IMemoryBudgetProvider" /> mock with a fixed available-byte value.</summary>
    /// <param name="availableBytes">Fixed the available memory budget in bytes.</param>
    internal static IMemoryBudgetProvider CreateMemoryBudget(long availableBytes)
    {
        var expectations = new IMemoryBudgetProviderCreateExpectations();
        _ = expectations.Setups.GetTotalAvailableBytes().ReturnValue(availableBytes);
        return expectations.Instance();
    }

    /// <summary>Creates an <see cref="IReplicaMembership" /> mock that counts every node a member of every group.</summary>
    /// <returns>The membership.</returns>
    internal static IReplicaMembership CreateReplicaMembers()
    {
        var expectations = new IReplicaMembershipCreateExpectations();
        _ = expectations.Setups.IsMember(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(true);
        return expectations.Instance();
    }

    /// <summary>Creates a journal event loop host over a fresh pending-append registry, and the state its callbacks record.</summary>
    /// <param name="queuedAppends">The queued-append counter the journal thread decrements, or <see langword="null" /> to ignore decrements.</param>
    /// <returns>The host mock and the recorded state.</returns>
    internal static EventLoopHostDouble CreateEventLoopHost(MutableInt32? queuedAppends = null)
    {
        var state = new EventLoopHostState { QueuedAppends = queuedAppends };
        var expectations = new IJournalEventLoopHostCreateExpectations();
        _ = expectations.Setups.PendingAppends.Gets().ReturnValue(state.PendingAppends);
        _ = expectations.Setups.CompleteDurabilityCheckpoint(Arg.Any<JournalWorkItem>())
                        .Callback(static item => _ = item.Ack?.TrySetResult());
        _ = expectations.Setups.DecrementQueuedAppends().Callback(state.DecrementQueuedAppends);
        _ = expectations.Setups.FailPipeline(Arg.Any<Exception>()).Callback(state.RecordPipelineFailure);
        _ = expectations.Setups.PublishRoll(Arg.Any<int>()).Callback(_ => state.RecordRollPublished());
        return new EventLoopHostDouble(expectations.Instance(), state);
    }

    /// <summary>A journal event loop host and the state its recorded callbacks left behind.</summary>
    /// <param name="Host">The generated host mock.</param>
    /// <param name="State">The state the host recorded.</param>
    internal sealed record EventLoopHostDouble(IJournalEventLoopHost Host, EventLoopHostState State);

    /// <summary>What the journal thread reported through the host: the last pipeline failure and the roll publications.</summary>
    [Mutable]
    internal sealed class EventLoopHostState
    {
        /// <summary>Gets the registry the journal thread tracks its appends in.</summary>
        internal PendingAppendRegistry PendingAppends { get; } = new();

        /// <summary>Gets the reason of the last pipeline failure the journal thread reported, if any.</summary>
        internal Exception? PipelineFailure { get; private set; }

        /// <summary>Gets the number of roll publications the journal thread made.</summary>
        internal int RollsPublished { get; private set; }

        /// <summary>Gets or sets the callback run on every roll publication, standing in for the manifest roll thread.</summary>
        internal Action? OnRollPublished { get; set; }

        /// <summary>Gets or sets the counter decremented on every dequeued append.</summary>
        internal MutableInt32? QueuedAppends { get; set; }

        /// <summary>Records the reason the journal thread reported as a pipeline failure.</summary>
        /// <param name="reason">The reason.</param>
        internal void RecordPipelineFailure(Exception reason) => PipelineFailure = reason;

        /// <summary>Records one roll publication.</summary>
        internal void RecordRollPublished()
        {
            RollsPublished++;
            OnRollPublished?.Invoke();
        }

        /// <summary>Decrements the queued-append counter when the test gave one.</summary>
        internal void DecrementQueuedAppends()
        {
            if (QueuedAppends != null)
                _ = Interlocked.Decrement(ref QueuedAppends.Value);
        }
    }
}
