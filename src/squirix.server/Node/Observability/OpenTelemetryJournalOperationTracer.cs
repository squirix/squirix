using System;
using System.Diagnostics;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// OpenTelemetry-backed <see cref="IJournalOperationTracer" /> implementation.
/// </summary>
internal sealed class OpenTelemetryJournalOperationTracer : IJournalOperationTracer
{
    /// <inheritdoc />
    public IJournalOperationTraceScope? Begin(JournalOperationKind kind, in JournalOperationTraceContext context)
    {
        var activity = ActivitySourceHolder.StartInternal(GetSpanName(kind));
        if (activity is null)
            return null;

        ApplyContextTags(activity, in context);
        return new OpenTelemetryJournalOperationTraceScope(activity);
    }

    private static void ApplyContextTags(Activity activity, in JournalOperationTraceContext context)
    {
        if (context.Key is not null)
            _ = activity.SetTag("journal.key", context.Key);
        if (!string.IsNullOrEmpty(context.Namespace))
            _ = activity.SetTag("journal.namespace", context.Namespace);
        if (context.PayloadBytes is { } payloadBytes)
            _ = activity.SetTag("journal.bytes_payload", payloadBytes);
        _ = activity.SetTag("journal.strict_fsync", true);
        if (context.GroupCommitEnabled is { } groupCommitEnabled)
            _ = activity.SetTag("journal.group_commit", groupCommitEnabled);
    }

    private static string GetSpanName(JournalOperationKind kind) => kind switch
    {
        JournalOperationKind.Remove => "journal.remove",
        JournalOperationKind.RemoveExpiration => "journal.remove_expiration",
        JournalOperationKind.TouchExpiration => "journal.touch_expiration",
        JournalOperationKind.Put => "journal.put",
        JournalOperationKind.AwaitDurabilityCommit => "journal.await_durability",
        JournalOperationKind.WaitForStartup => "journal.wait_startup",
        JournalOperationKind.MaintenanceExclusive => "journal.maintenance",
        JournalOperationKind.SnapshotCut => "journal.snapshot_cut",
        JournalOperationKind.UnderSnapshotBarrier => "journal.snapshot_barrier",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported journal operation kind."),
    };

    private sealed class OpenTelemetryJournalOperationTraceScope : IJournalOperationTraceScope
    {
        private readonly Activity _activity;

        public OpenTelemetryJournalOperationTraceScope(Activity activity)
        {
            _activity = activity;
        }

        public void Dispose() => _activity.Dispose();

        public void SetFrameBytes(int payloadBytes)
        {
            _ = _activity.SetTag("journal.frame.payload_bytes", payloadBytes);
            _ = _activity.SetTag("journal.frame.total_bytes", JournalFraming.FrameHeaderSize + payloadBytes + JournalFraming.FrameFooterSize);
        }
    }
}
