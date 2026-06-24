namespace Squirix.Server.Node;

/// <summary>
/// Classifies which cancellation source is authoritative for an <see cref="System.OperationCanceledException" /> path.
/// </summary>
internal enum CancellationScenarioKind
{
    /// <summary>The outer caller token is canceled.</summary>
    CallerCanceled = 0,

    /// <summary>The operation-level effective token is canceled while the caller token is not.</summary>
    OperationDeadlineExceeded = 1,

    /// <summary>The per-attempt composite token fired while the operation effective token is not canceled.</summary>
    PerAttemptTimedOut = 2,

    /// <summary>Cancellation occurred without matching the structured sources above.</summary>
    UnknownCancellation = 3,
}
