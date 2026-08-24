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

    private static Activity AssertActivity(string expectedDisplayName)
    {
        var activity = Activity.Current;
        Assert.NotNull(activity);
        Assert.Equal(expectedDisplayName, activity.DisplayName);
        return activity;
    }
}
