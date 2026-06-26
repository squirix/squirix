using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Observability;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Helpers for tracing journal coordinator operations through <see cref="IJournalOperationTracer" />.
/// </summary>
internal static class JournalCoordinatorTracing
{
    public static JournalOperationTraceContext ForKey(CacheKey key) => new()
    {
        Key = key.Key,
        Namespace = string.IsNullOrEmpty(key.Namespace) ? null : key.Namespace,
    };

    public static JournalOperationTraceContext WithDurability(in JournalOperationTraceContext context, IJournalCoordinator coordinator) => context with
    {
        GroupCommitEnabled = coordinator.IsJournalGroupCommitEnabled,
    };
}
