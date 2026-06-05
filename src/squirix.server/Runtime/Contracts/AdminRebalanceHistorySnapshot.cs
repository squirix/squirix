namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Empty core snapshot for the rebalance-history admin endpoint.
/// </summary>
internal sealed class AdminRebalanceHistorySnapshot
{
    public required AdminRebalanceHistoryItem[] Events { get; init; }

    public required int Retention { get; init; }
}
