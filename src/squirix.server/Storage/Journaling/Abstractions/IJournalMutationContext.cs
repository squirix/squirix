namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Operation metadata and acceptance notifications used by the cache journal.</summary>
internal interface IJournalMutationContext
{
    /// <summary>Gets the operation id to stamp, or null when this mutation must remain unstamped.</summary>
    string? OperationId { get; }

    /// <summary>Gets the fingerprint associated with the operation id.</summary>
    string? Fingerprint { get; }

    /// <summary>Gets a value indicating whether the operation already appended its outcome and must refuse further mutations.</summary>
    bool HasAppendedOutcome { get; }

    /// <summary>Reports a stamped mutation only after its frame was accepted by the journal ring.</summary>
    void NotifyMutationStamped();

    /// <summary>Reports an unstamped mutation accepted by the journal ring.</summary>
    void NotifyMutationApplied();
}
