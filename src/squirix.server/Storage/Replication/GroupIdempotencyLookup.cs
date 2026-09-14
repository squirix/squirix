namespace Squirix.Server.Storage.Replication;

/// <summary>Result of an idempotency lookup.</summary>
internal enum GroupIdempotencyLookup
{
    /// <summary>The operation is unknown to the retained idempotency state.</summary>
    Miss = 0,

    /// <summary>The operation was found with a matching fingerprint; its outcome may be returned.</summary>
    Found = 1,

    /// <summary>The operation id exists but the request fingerprint differs; the reuse is rejected.</summary>
    Mismatch = 2,

    /// <summary>The operation id exists with a matching fingerprint but the record is not yet resolved.</summary>
    Unresolved = 3,
}
