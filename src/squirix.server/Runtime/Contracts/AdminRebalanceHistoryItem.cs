using System;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Rebalance-history event DTO for the admin response shape.
/// </summary>
internal readonly record struct AdminRebalanceHistoryItem(
    long Sequence,
    DateTime TimestampUtc,
    string Action,
    string? NodeId,
    string[] PreviousMembers,
    string[] CurrentMembers,
    int PreviousVirtualNodes,
    int CurrentVirtualNodes);
