namespace Squirix.Server.Node;

/// <summary>
/// Classifies which cancellation source is authoritative for an <see cref="System.OperationCanceledException" /> path.
/// </summary>
internal enum CancellationScenarioKind
{
    /// <summary>The outer caller token is canceled.</summary>
    CallerCanceled,

    /// <summary>The operation-level effective token is canceled while the caller token is not.</summary>
    OperationDeadlineExceeded,

    /// <summary>The per-attempt composite token fired while the operation effective token is not canceled.</summary>
    PerAttemptTimedOut,

    /// <summary>Cancellation occurred without matching the structured sources above.</summary>
    UnknownCancellation,
}
