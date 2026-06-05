namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Starts trace scopes for journal writer operations.
/// </summary>
internal interface IJournalOperationTracer
{
    /// <summary>
    /// Begins a trace scope for <paramref name="kind" /> when tracing is enabled.
    /// </summary>
    /// <param name="kind">Operation being traced.</param>
    /// <param name="context">Optional operation tags.</param>
    /// <returns>An active scope, or <see langword="null" /> when tracing is disabled.</returns>
    IJournalOperationTraceScope? Begin(JournalOperationKind kind, in JournalOperationTraceContext context);
}
