namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of restoring a resolved outcome rebuilt from a committed log entry.</summary>
internal enum GroupOutcomeRestoreResult
{
    /// <summary>The outcome was stored, in a free place, in the place of an older retained outcome, or over an older outcome of the same identity.</summary>
    Restored = 0,

    /// <summary>The identity is already retained by a pin or by an outcome of the same or a newer log index, which is kept.</summary>
    Retained = 1,

    /// <summary>The outcome is past its retention window and is skipped.</summary>
    Expired = 2,

    /// <summary>The store is full and holds no resolved outcome with an older log index to give way.</summary>
    Full = 3,
}
