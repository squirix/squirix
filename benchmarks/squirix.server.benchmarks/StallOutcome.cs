namespace Squirix.Server.Benchmarks;

/// <summary>What a writer saw for one durable mutation during the stall measurement.</summary>
internal static class StallOutcome
{
    /// <summary>The mutation committed before the client gave up.</summary>
    internal const int Ok = 0;

    /// <summary>The server canceled the mutation while it waited for the mutation gate: a definite failure, nothing was appended.</summary>
    internal const int CanceledAtGate = 1;

    /// <summary>The server canceled the mutation after it took the gate but before its frame was appended: a definite failure.</summary>
    internal const int CanceledInAppend = 2;

    /// <summary>The client gave up while the frame was already appended and the server kept waiting: the outcome is unknown.</summary>
    internal const int TimedOutFrameQueued = 3;

    /// <summary>The client gave up before the frame was appended.</summary>
    internal const int TimedOutNotAppended = 4;

    /// <summary>The mutation failed with another error.</summary>
    internal const int Error = 5;

    /// <summary>The number of outcome values.</summary>
    internal const int Count = 6;

    /// <summary>Gets the table column title of an outcome.</summary>
    /// <param name="outcome">The outcome value.</param>
    /// <returns>The title.</returns>
    internal static string Title(int outcome) => outcome switch
    {
        Ok => "ok",
        CanceledAtGate => "canceled at gate (definite)",
        CanceledInAppend => "canceled in append (definite)",
        TimedOutFrameQueued => "client timeout, frame queued (unknown)",
        TimedOutNotAppended => "client timeout, not appended",
        Error => "error",
        _ => "unknown outcome",
    };
}
