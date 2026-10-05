using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Starts trace scopes for journal writer operations.</summary>
internal interface IJournalOperationTracer
{
    /// <summary>Gets a value indicating whether <see cref="Begin" /> may start a scope, without starting one.</summary>
    /// <remarks>Lets a caller skip tracing on a path where the scope would have to start and end around an await.</remarks>
    bool IsEnabled { get; }

    /// <summary>Begins a trace scope for <paramref name="kind" /> when tracing is enabled.</summary>
    /// <param name="kind">Operation being traced.</param>
    /// <param name="context">Optional operation tags.</param>
    /// <returns>An active scope, or <see langword="null" /> when tracing is disabled.</returns>
    IJournalOperationTraceScope? Begin(JournalOperationKind kind, in JournalOperationTraceContext? context);
}
