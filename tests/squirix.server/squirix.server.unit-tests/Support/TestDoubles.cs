using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Factories for shared test doubles.</summary>
internal static class TestDoubles
{
    /// <summary>Creates an <see cref="IMemoryBudgetProvider" /> mock with a fixed available-byte value.</summary>
    /// <param name="availableBytes">Fixed the available memory budget in bytes.</param>
    internal static IMemoryBudgetProvider CreateMemoryBudget(long availableBytes)
    {
        var expectations = IMemoryBudgetProvider.Mock(MockBehavior.Strict);
        _ = expectations.GetTotalAvailableBytes().Returns(availableBytes);
        return expectations.Object;
    }

    /// <summary>Creates an <see cref="IReplicaMembership" /> mock that counts every node a member of every group.</summary>
    /// <returns>The membership.</returns>
    internal static IReplicaMembership CreateReplicaMembers()
    {
        var expectations = IReplicaMembership.Mock(MockBehavior.Strict);
        _ = expectations.IsMember(Arg.Any<string>(), Arg.Any<string>()).Returns(true);
        return expectations.Object;
    }

    /// <summary>Creates a cache mock whose encoded-length writes dispatch to the corresponding configured writes.</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <returns>The strict cache mock.</returns>
    internal static Mock<ILogicalNamespacedCache<T>> CreateCacheWithEntryWriteOverloads<T>()
    {
        var cache = ILogicalNamespacedCache<T>.Mock(MockBehavior.Strict);
        _ = cache.SetEntryAsync(
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<NodeCacheEntry<T>>(),
            Arg.Any<int?>(),
            Arg.Any<CancellationToken>()).ReturnsAsync(
            (operationId, cacheName, key, entry, _, token) => cache.Object.SetEntryAsync(operationId, cacheName, key, entry, token));
        _ = cache.TryAddEntryAsync(
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<NodeCacheEntry<T>>(),
            Arg.Any<int?>(),
            Arg.Any<CancellationToken>()).ReturnsAsync(
            (operationId, cacheName, key, entry, _, token) => cache.Object.TryAddEntryAsync(operationId, cacheName, key, entry, token));
        return cache;
    }

    /// <summary>Creates a journal event loop host over a fresh pending-append registry, and the state its callbacks record.</summary>
    /// <param name="queuedAppends">The queued-append counter the journal thread decrements, or <see langword="null" /> to ignore decrements.</param>
    /// <param name="checkFailure">Optional observer of the event loop's failure check.</param>
    /// <returns>The host mock and the recorded state.</returns>
    internal static EventLoopHostDouble CreateEventLoopHost(MutableInt32? queuedAppends = null, Action? checkFailure = null)
    {
        var state = new EventLoopHostState { QueuedAppends = queuedAppends };
        var expectations = IJournalEventLoopHost.Mock(MockBehavior.Strict);
        _ = expectations.PendingAppends.Returns(state.PendingAppends);
        _ = expectations.CompleteDurabilityCheckpoint(Arg.Any<JournalWorkItem>())
                        .Callback(static item => _ = item.Ack?.TrySetResult());
        _ = expectations.DecrementQueuedAppends().Callback(state.DecrementQueuedAppends);
        _ = expectations.FailPipeline(Arg.Any<Exception>()).Callback(state.RecordPipelineFailure);
        _ = expectations.PublishRoll(Arg.Any<int>()).Callback(_ => state.RecordRollPublished());
        if (checkFailure != null)
            _ = expectations.ThrowIfJournalThreadFailed().Callback(checkFailure);

        return new EventLoopHostDouble(expectations.Object, state);
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

        /// <summary>Gets or sets the callback run on every pipeline failure report, before the failure is recorded.</summary>
        internal Action<Exception>? OnPipelineFailed { get; set; }

        /// <summary>Gets or sets the callback run on every roll publication, standing in for the manifest roll thread.</summary>
        internal Action? OnRollPublished { get; set; }

        /// <summary>Gets or sets the counter decremented on every dequeued append.</summary>
        internal MutableInt32? QueuedAppends { get; set; }

        /// <summary>Records the reason the journal thread reported as a pipeline failure.</summary>
        /// <param name="reason">The reason.</param>
        internal void RecordPipelineFailure(Exception reason)
        {
            OnPipelineFailed?.Invoke(reason);
            PipelineFailure = reason;
        }

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
