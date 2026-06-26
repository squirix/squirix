using System.Collections.Generic;
using Squirix.Server.Storage.Journaling.Observability;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Captures <see cref="IJournalOperationTracer.Begin" /> calls for decorator unit tests.
/// </summary>
internal sealed class RecordingJournalOperationTracer : IJournalOperationTracer
{
    public List<(JournalOperationKind Kind, JournalOperationTraceContext Context)> BeginCalls { get; } = [];

    public IJournalOperationTraceScope Begin(JournalOperationKind kind, in JournalOperationTraceContext context)
    {
        BeginCalls.Add((kind, context));
        return new RecordingScope();
    }

    private sealed class RecordingScope : IJournalOperationTraceScope
    {
        public void Dispose()
        {
        }
    }
}
