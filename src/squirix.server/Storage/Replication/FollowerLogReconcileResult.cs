using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of a durable follower-tail reconciliation.</summary>
/// <param name="Success">Whether the requested tail is durably absent.</param>
/// <param name="RefusalCode">Stable refusal marker, or an empty string on success.</param>
/// <param name="LastLogIndex">Last retained durable log index.</param>
/// <param name="ReleasedReservations">Number of idempotency reservations released after durable removal.</param>
/// <param name="Quarantined">Whether storage readiness was failed closed.</param>
[Immutable]
internal readonly record struct FollowerLogReconcileResult(
    bool Success,
    string RefusalCode,
    ulong LastLogIndex,
    int ReleasedReservations,
    bool Quarantined);
