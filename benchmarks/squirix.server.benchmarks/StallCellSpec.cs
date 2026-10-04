namespace Squirix.Server.Benchmarks;

/// <summary>One cell of the stall measurement matrix.</summary>
/// <param name="GroupCommit">Whether journal group commit is enabled.</param>
/// <param name="Writers">The number of concurrent writers (K).</param>
/// <param name="DeadlineSeconds">The client deadline of every mutation (D).</param>
/// <param name="StallSeconds">The simulated disk stall duration (S).</param>
/// <param name="Kind">The journal call the stall blocks.</param>
/// <param name="PreSeconds">The steady-state seconds before the stall.</param>
/// <param name="PostSeconds">The seconds measured after the stall ends.</param>
/// <param name="PayloadBytes">The PUT payload size.</param>
internal sealed record StallCellSpec(bool GroupCommit, int Writers, int DeadlineSeconds, int StallSeconds, JournalStallKind Kind, int PreSeconds, int PostSeconds, int PayloadBytes)
{
    /// <summary>Gets the group commit label used in tables.</summary>
    internal string GcLabel => GroupCommit ? "on" : "off";

    /// <summary>Gets the leading table cells identifying the cell.</summary>
    /// <returns>The cells text, with a trailing pipe.</returns>
    internal string Key() => $"| {GcLabel} | {StallSeconds} | {Writers} | {DeadlineSeconds} |";
}
