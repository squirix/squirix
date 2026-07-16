using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Helpers for tracing journal coordinator operations through <see cref="IJournalOperationTracer" />.
/// </summary>
internal static class JournalCoordinatorTracing
{
    internal static JournalOperationTraceContext ForKey(CacheKey key) => new()
    {
        Key = key.Key,
        Namespace = string.IsNullOrEmpty(key.Namespace) ? null : key.Namespace,
    };

    internal static JournalOperationTraceContext? WithDurability(in JournalOperationTraceContext? context, IJournalCoordinator coordinator)
    {
        if (context != null)
        {
            return context with
            {
                GroupCommitEnabled = coordinator.IsJournalGroupCommitEnabled,
            };
        }

        return null;
    }
}
