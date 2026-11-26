using System;
using System.Diagnostics;

namespace Squirix.Server.Logging;

/// <summary>Observes peer pre-vote phases using the trace identifier carried in existing peer metadata.</summary>
internal static class PreVoteTrace
{
    internal const string Operation = "squirix.peer.pre-vote";

    internal static event EventHandler<StageEventArgs>? Observed;

    internal static bool HasObservers => Observed != null;

    internal static Activity? Accept(string? parent)
    {
        if (parent == null || !HasObservers)
            return null;

        var activity = new Activity(Operation);
        _ = activity.SetParentId(parent);
        _ = activity.Start();
        return activity;
    }

    internal static void Mark(string phase)
    {
        var observer = Observed;
        if (observer != null && Activity.Current is { } activity && string.Equals(activity.OperationName, Operation, StringComparison.Ordinal))
            observer(null, new StageEventArgs(activity.TraceId, phase));
    }

    internal sealed class StageEventArgs : EventArgs
    {
        internal StageEventArgs(ActivityTraceId traceId, string phase)
        {
            TraceId = traceId;
            Phase = phase;
        }

        internal ActivityTraceId TraceId { get; }

        internal string Phase { get; }
    }
}
