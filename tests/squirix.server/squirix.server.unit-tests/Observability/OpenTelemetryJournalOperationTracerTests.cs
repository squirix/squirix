using System.Diagnostics;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Unit tests for <see cref="OpenTelemetryJournalOperationTracer" /> context tag mapping.
/// </summary>
[Immutable]
public sealed class OpenTelemetryJournalOperationTracerTests
{
    /// <summary>Ensures payload byte tags are applied when context carries payload size.</summary>
    [Fact]
    public void BeginAppliesPayloadAndFrameTotalTags()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            PayloadBytes = 128,
        };

        using var scope = journalTracer.Begin(JournalOperationKind.Put, in context);

        Assert.NotNull(scope);
        var activity = AssertActivity("journal.put");
        Assert.Equal("128", Assert.IsType<string>(activity.GetTagItem("journal.bytes_payload")));
        Assert.Equal("136", Assert.IsType<string>(activity.GetTagItem("journal.frame.total_bytes")));
    }

    /// <summary>Ensures durability settings on <see cref="JournalOperationTraceContext" /> are exported as span tags.</summary>
    [Fact]
    public void BeginTagsStrictFsyncAndGroupCommit()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            GroupCommitEnabled = false,
        };

        using var scope = journalTracer.Begin(JournalOperationKind.Put, in context);

        Assert.NotNull(scope);
        var activity = AssertActivity("journal.put");
        Assert.Equal(ActivityTagValues.True, Assert.IsType<string>(activity.GetTagItem("journal.strict_fsync")));
        Assert.Equal(ActivityTagValues.False, Assert.IsType<string>(activity.GetTagItem("journal.group_commit")));
    }

    /// <summary>Ensures unset durability settings do not emit durability span tags.</summary>
    [Fact]
    public void BeginOmitsTagsForNullContextValues()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();
        using var scope = journalTracer.Begin(JournalOperationKind.Put, null);

        Assert.NotNull(scope);
        var activity = AssertActivity("journal.put");
        Assert.Null(activity.GetTagItem("journal.group_commit"));
    }

    /// <summary>Ensures every journal operation kind maps to a span name, including write-ahead intents.</summary>
    [Fact]
    public void BeginMapsAllOperationKinds()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();

        AssertSpanName(journalTracer, JournalOperationKind.Put, "journal.put");
        AssertSpanName(journalTracer, JournalOperationKind.Remove, "journal.remove");
        AssertSpanName(journalTracer, JournalOperationKind.RemoveExpiration, "journal.remove_expiration");
        AssertSpanName(journalTracer, JournalOperationKind.TouchExpiration, "journal.touch_expiration");
        AssertSpanName(journalTracer, JournalOperationKind.IdempotencyOutcome, "journal.idempotency_outcome");
        AssertSpanName(journalTracer, JournalOperationKind.IdempotencyStarted, "journal.idempotency_started");
        AssertSpanName(journalTracer, JournalOperationKind.AwaitDurabilityCommit, "journal.await_durability");
        AssertSpanName(journalTracer, JournalOperationKind.WaitForStartup, "journal.wait_startup");
        AssertSpanName(journalTracer, JournalOperationKind.MaintenanceExclusive, "journal.maintenance");
        AssertSpanName(journalTracer, JournalOperationKind.SnapshotCut, "journal.snapshot_cut");
        AssertSpanName(journalTracer, JournalOperationKind.UnderSnapshotBarrier, "journal.snapshot_barrier");
    }

    private static Activity AssertActivity(string expectedDisplayName)
    {
        var activity = Activity.Current;
        Assert.NotNull(activity);
        Assert.Equal(expectedDisplayName, activity.DisplayName);
        return activity;
    }

    private static void AssertSpanName(IJournalOperationTracer journalTracer, JournalOperationKind kind, string expectedDisplayName)
    {
        using var scope = journalTracer.Begin(kind, null);

        Assert.NotNull(scope);
        _ = AssertActivity(expectedDisplayName);
    }
}
