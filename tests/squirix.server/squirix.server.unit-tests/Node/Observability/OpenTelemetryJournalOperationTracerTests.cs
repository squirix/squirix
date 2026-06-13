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
        using var listener = CreateSquirixSamplingListener();

        var tracer = new OpenTelemetryJournalOperationTracer();
        var context = new JournalOperationTraceContext
        {
            GroupCommitEnabled = false,
        };

        using var scope = tracer.Begin(JournalOperationKind.Put, in context);

        Assert.NotNull(scope);
        var activity = AssertActivity("journal.put");
        Assert.Equal(true, activity.GetTagItem("journal.strict_fsync"));
        Assert.Equal(false, activity.GetTagItem("journal.group_commit"));
    }

    /// <summary>
    /// Ensures unset durability settings do not emit durability span tags.
    /// </summary>
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

    /// <summary>
    /// Enables sampling so the Squirix activity source returns a non-null activity.
    /// </summary>
    private static ActivityListener CreateSquirixSamplingListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == ActivitySourceHolder.SourceName,
            Sample = static (ref _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
