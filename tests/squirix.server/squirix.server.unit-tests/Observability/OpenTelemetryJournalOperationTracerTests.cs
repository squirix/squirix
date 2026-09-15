using System.Diagnostics;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Unit tests for <see cref="OpenTelemetryJournalOperationTracer" /> context tag mapping.</summary>
[Immutable]
public sealed class OpenTelemetryJournalOperationTracerTests
{
    /// <summary>Ensures payload byte tags are applied when context carries payload size.</summary>
    [Test]
    public async Task BeginAppliesPayloadAndFrameTotalTags()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            PayloadBytes = 128,
        };

        using var scope = journalTracer.Begin(JournalOperationKind.Put, in context);

        _ = await Assert.That(scope).IsNotNull();
        var activity = await AssertActivity("journal.put");
        var payloadTag = await Assert.That(activity.GetTagItem("journal.bytes_payload")).IsTypeOf<string>();
        _ = await Assert.That(payloadTag).IsEqualTo("128");
        var frameTag = await Assert.That(activity.GetTagItem("journal.frame.total_bytes")).IsTypeOf<string>();
        _ = await Assert.That(frameTag).IsEqualTo("136");
    }

    /// <summary>Ensures every journal operation kind maps to a span name, including write-ahead intents.</summary>
    [Test]
    public async Task BeginMapsAllOperationKinds()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();

        await AssertSpanName(journalTracer, JournalOperationKind.Put, "journal.put");
        await AssertSpanName(journalTracer, JournalOperationKind.Remove, "journal.remove");
        await AssertSpanName(journalTracer, JournalOperationKind.RemoveExpiration, "journal.remove_expiration");
        await AssertSpanName(journalTracer, JournalOperationKind.TouchExpiration, "journal.touch_expiration");
        await AssertSpanName(journalTracer, JournalOperationKind.IdempotencyOutcome, "journal.idempotency_outcome");
        await AssertSpanName(journalTracer, JournalOperationKind.IdempotencyStarted, "journal.idempotency_started");
        await AssertSpanName(journalTracer, JournalOperationKind.AwaitDurabilityCommit, "journal.await_durability");
        await AssertSpanName(journalTracer, JournalOperationKind.WaitForStartup, "journal.wait_startup");
        await AssertSpanName(journalTracer, JournalOperationKind.MaintenanceExclusive, "journal.maintenance");
        await AssertSpanName(journalTracer, JournalOperationKind.SnapshotCut, "journal.snapshot_cut");
        await AssertSpanName(journalTracer, JournalOperationKind.UnderSnapshotBarrier, "journal.snapshot_barrier");
    }

    /// <summary>Ensures unset durability settings do not emit durability span tags.</summary>
    [Test]
    public async Task BeginOmitsTagsForNullContextValues()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();
        using var scope = journalTracer.Begin(JournalOperationKind.Put, null);

        _ = await Assert.That(scope).IsNotNull();
        var activity = await AssertActivity("journal.put");
        _ = await Assert.That(activity.GetTagItem("journal.group_commit")).IsNull();
    }

    /// <summary>Ensures durability settings on <see cref="JournalOperationTraceContext" /> are exported as span tags.</summary>
    [Test]
    public async Task BeginTagsStrictFsyncAndGroupCommit()
    {
        using var listener = ActivityListenerTestKit.CreateSquirixSamplingListener();

        IJournalOperationTracer journalTracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            GroupCommitEnabled = false,
        };

        using var scope = journalTracer.Begin(JournalOperationKind.Put, in context);

        _ = await Assert.That(scope).IsNotNull();
        var activity = await AssertActivity("journal.put");
        var fsyncTag = await Assert.That(activity.GetTagItem("journal.strict_fsync")).IsTypeOf<string>();
        _ = await Assert.That(fsyncTag).IsEqualTo(ActivityTagValues.True);
        var groupCommitTag = await Assert.That(activity.GetTagItem("journal.group_commit")).IsTypeOf<string>();
        _ = await Assert.That(groupCommitTag).IsEqualTo(ActivityTagValues.False);
    }

    private static async Task<Activity> AssertActivity(string expectedDisplayName)
    {
        var activity = Activity.Current;
        _ = await Assert.That(activity).IsNotNull();
        _ = await Assert.That(activity.DisplayName).IsEqualTo(expectedDisplayName);
        return activity;
    }

    private static async Task AssertSpanName(IJournalOperationTracer journalTracer, JournalOperationKind kind, string expectedDisplayName)
    {
        using var scope = journalTracer.Begin(kind, null);

        _ = await Assert.That(scope).IsNotNull();
        _ = await AssertActivity(expectedDisplayName);
    }
}
