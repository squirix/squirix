using System;
using System.Diagnostics;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Unit tests for <see cref="OpenTelemetryJournalOperationTracer" /> context tag mapping.
/// </summary>
public sealed class OpenTelemetryJournalOperationTracerTests
{
    /// <summary>
    /// Ensures durability settings on <see cref="JournalOperationTraceContext" /> are exported as span tags.
    /// </summary>
    [Fact]
    public void BeginAppliesStrictFsyncAndGroupCommitTags()
    {
        Activity? captured = null;
        using var listener = CreateSquirixActivityListener(activity =>
        {
            if (activity.DisplayName == "journal.put")
                captured = activity;
        });

        var tracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            StrictFsync = true,
            GroupCommitEnabled = false,
        };

        using var scope = tracer.Begin(JournalOperationKind.Put, in context);

        Assert.NotNull(scope);
        Assert.NotNull(captured);
        Assert.Equal("journal.put", captured.DisplayName);
        Assert.Equal(true, captured.GetTagItem("journal.strict_fsync"));
        Assert.Equal(false, captured.GetTagItem("journal.group_commit"));
    }

    /// <summary>
    /// Ensures unset durability settings do not emit durability span tags.
    /// </summary>
    [Fact]
    public void BeginOmitsDurabilityTagsWhenContextValuesAreNull()
    {
        Activity? captured = null;
        using var listener = CreateSquirixActivityListener(activity =>
        {
            if (activity.DisplayName == "journal.put")
                captured = activity;
        });

        var tracer = new OpenTelemetryJournalOperationTracer();
        using var scope = tracer.Begin(JournalOperationKind.Put, default);

        Assert.NotNull(scope);
        Assert.NotNull(captured);
        Assert.Null(captured.GetTagItem("journal.strict_fsync"));
        Assert.Null(captured.GetTagItem("journal.group_commit"));
    }

    private static ActivityListener CreateSquirixActivityListener(Action<Activity> onStarted)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == "Squirix",
            Sample = static (ref _) => ActivitySamplingResult.AllData,
            ActivityStarted = onStarted,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
