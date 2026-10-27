namespace Squirix.Server.Benchmarks;

/// <summary>The timeline of one durable mutation issued by a stall measurement writer.</summary>
internal sealed class StallRequest
{
    /// <summary>Gets when the client issued the request, in milliseconds since the run started.</summary>
    internal double IssueMs { get; init; }

    /// <summary>Gets or sets when the client stopped waiting.</summary>
    internal double ClientEndMs { get; set; }

    /// <summary>Gets or sets a value indicating whether the mutation finally committed on the server, even after the client gave up.</summary>
    internal bool CommittedLater { get; set; }

    /// <summary>Gets or sets what the writer saw, a <see cref="StallOutcome" /> value.</summary>
    internal int Outcome { get; set; }

    /// <summary>Gets or sets when the server finished with the mutation.</summary>
    internal double ServerEndMs { get; set; }
}
