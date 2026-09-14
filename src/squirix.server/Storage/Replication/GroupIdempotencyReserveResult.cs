namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of an idempotency reservation attempt.</summary>
internal enum GroupIdempotencyReserveResult
{
    /// <summary>The record was reserved or already reserved with a matching fingerprint.</summary>
    Success = 0,

    /// <summary>An existing record exists with a different fingerprint; the reservation is rejected.</summary>
    FingerprintMismatch = 1,

    /// <summary>The store is at capacity and the key is new; the reservation is rejected.</summary>
    CapacityExceeded = 2,
}
