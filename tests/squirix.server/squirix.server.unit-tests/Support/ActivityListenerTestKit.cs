using System;
using System.Diagnostics;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Registers an activity listener for the shared Squirix activity source.</summary>
internal static class ActivityListenerTestKit
{
    /// <summary>Creates and registers a listener that samples all Squirix activities.</summary>
    /// <param name="sampleUsingParentId">When <see langword="true" />, also samples activities created from a parent id.</param>
    /// <returns>A disposable activity listener.</returns>
    internal static ActivityListener CreateSquirixSamplingListener(bool sampleUsingParentId = false)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => string.Equals(source.Name, ActivitySourceHolder.SourceName, StringComparison.OrdinalIgnoreCase),
            Sample = static (ref _) => ActivitySamplingResult.AllData,
        };

        if (sampleUsingParentId)
            listener.SampleUsingParentId = static (ref _) => ActivitySamplingResult.AllData;

        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
