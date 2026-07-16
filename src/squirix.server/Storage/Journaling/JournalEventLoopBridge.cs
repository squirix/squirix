using System;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Forwards <see cref="IJournalEventLoopHost" /> callbacks from <see cref="JournalEventLoop" />
/// to <see cref="JournalCoordinator" /> without the coordinator implementing the interface directly.
/// </summary>
internal sealed class JournalEventLoopBridge : IJournalEventLoopHost
{
    private readonly JournalCoordinator _coordinator;
    private readonly JournalCoordinatorDurabilityPipeline _durabilityPipeline;
    private readonly ManifestRollPublisher _manifestRollPublisher;

    internal JournalEventLoopBridge(JournalCoordinator coordinator, JournalCoordinatorDurabilityPipeline durabilityPipeline, ManifestRollPublisher manifestRollPublisher)
    {
        _coordinator = coordinator;
        _durabilityPipeline = durabilityPipeline;
        _manifestRollPublisher = manifestRollPublisher;
    }

    void IJournalEventLoopHost.CompleteDurabilityCheckpoint() => _durabilityPipeline.CompleteDurabilityCheckpointOnJournalThread();

    void IJournalEventLoopHost.DecrementQueuedAppends() => _ = Interlocked.Decrement(ref _coordinator.QueuedAppendsCounter.Value);

    void IJournalEventLoopHost.FailPipeline(Exception reason) => _durabilityPipeline.FailJournalPipeline(reason);

    void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex) => _manifestRollPublisher.PublishRoll(
        targetSegmentIndex,
        Volatile.Read(ref _coordinator.NextSequenceField),
        () => _durabilityPipeline.OnManifestRollSucceeded());

    int IJournalEventLoopHost.ReadQueuedAppends() => Volatile.Read(ref _coordinator.QueuedAppendsCounter.Value);

    void IJournalEventLoopHost.SetNextSequence(ulong value) => Volatile.Write(ref _coordinator.NextSequenceField, value);

    void IJournalEventLoopHost.ThrowIfJournalThreadFailed() => _durabilityPipeline.ThrowIfJournalThreadFailed();
}
