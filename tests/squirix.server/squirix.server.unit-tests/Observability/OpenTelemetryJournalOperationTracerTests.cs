using System;
using System.Diagnostics;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Journaling.Observability;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Unit tests for <see cref="OpenTelemetryJournalOperationTracer" /> context tag mapping.
/// </summary>
public sealed class OpenTelemetryJournalOperationTracerTests
{
    /// <summary>Ensures payload byte tags are applied when context carries payload size.</summary>
    [Fact]
    public void BeginAppliesPayloadAndFrameTotalTags()
    {
        using var listener = CreateSquirixSamplingListener();

        var tracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            PayloadBytes = 128,
        };

        using var scope = tracer.Begin(JournalOperationKind.Put, in context);

        Assert.NotNull(scope);
        var activity = AssertActivity("journal.put");
        Assert.Equal(128, Assert.IsType<int>(activity.GetTagItem("journal.bytes_payload")));
        Assert.Equal(136, Assert.IsType<int>(activity.GetTagItem("journal.frame.total_bytes")));
    }

    /// <summary>Ensures durability settings on <see cref="JournalOperationTraceContext" /> are exported as span tags.</summary>
    [Fact]
    public void BeginAppliesStrictFsyncAndGroupCommitTags()
    {
        using var listener = CreateSquirixSamplingListener();

        var tracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            GroupCommitEnabled = false,
        };

        using var scope = tracer.Begin(JournalOperationKind.Put, in context);

        Assert.NotNull(scope);
        var activity = AssertActivity("journal.put");
        Assert.True(Assert.IsType<bool>(activity.GetTagItem("journal.strict_fsync")));
        Assert.False(Assert.IsType<bool>(activity.GetTagItem("journal.group_commit")));
    }

    /// <summary>Ensures unset durability settings do not emit durability span tags.</summary>
    [Fact]
    public void BeginOmitsDurabilityTagsWhenContextValuesAreNull()
    {
        using var listener = CreateSquirixSamplingListener();

        var tracer = new OpenTelemetryJournalOperationTracer();
        using var scope = tracer.Begin(JournalOperationKind.Put, default);

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

    /// <summary>Enables sampling so the Squirix activity source returns a non-null activity.</summary>
    private static ActivityListener CreateSquirixSamplingListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => string.Equals(source.Name, ActivitySourceHolder.SourceName, StringComparison.OrdinalIgnoreCase),
            Sample = static (ref _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
