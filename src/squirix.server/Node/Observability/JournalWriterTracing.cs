using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Helpers for tracing journal coordinator operations through <see cref="IJournalOperationTracer" />.
/// </summary>
internal static class JournalWriterTracing
{
    public static JournalOperationTraceContext ForKey(CacheKey key) => new()
    {
        Key = key.Key,
        Namespace = string.IsNullOrEmpty(key.Namespace) ? null : key.Namespace,
    };

    public static JournalOperationTraceContext ForKey(string key, string? cacheNamespace = null) => new()
    {
        Key = key,
        Namespace = string.IsNullOrEmpty(cacheNamespace) ? null : cacheNamespace,
    };

    public static async ValueTask TraceAsync(
        IJournalOperationTracer tracer,
        JournalOperationKind kind,
        JournalOperationTraceContext context,
        Func<ValueTask> action,
        Action<IJournalOperationTraceScope?>? onSuccess = null)
    {
        using var scope = tracer.Begin(kind, in context);
        await action().ConfigureAwait(false);
        onSuccess?.Invoke(scope);
    }

    public static async ValueTask<TResult> TraceAsync<TResult>(
        IJournalOperationTracer tracer,
        JournalOperationKind kind,
        JournalOperationTraceContext context,
        Func<ValueTask<TResult>> action)
    {
        using var scope = tracer.Begin(kind, in context);
        return await action().ConfigureAwait(false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void TraceFrameBytes(IJournalOperationTraceScope? scope, int payloadBytes) => scope?.SetFrameBytes(payloadBytes);

    public static JournalOperationTraceContext WithDurability(in JournalOperationTraceContext context, JournalWriter writer) => context with
    {
        StrictFsync = writer.StrictFsync,
        GroupCommitEnabled = writer.IsJournalGroupCommitEnabled,
    };
}
