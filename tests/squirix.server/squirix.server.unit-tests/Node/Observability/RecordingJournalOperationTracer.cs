using System.Collections.Generic;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Captures <see cref="IJournalOperationTracer.Begin" /> calls for decorator unit tests.
/// </summary>
internal sealed class RecordingJournalOperationTracer : IJournalOperationTracer
{
    public List<(JournalOperationKind Kind, JournalOperationTraceContext Context)> BeginCalls { get; } = [];

    public List<int> FramePayloadBytes { get; } = [];

    public IJournalOperationTraceScope Begin(JournalOperationKind kind, in JournalOperationTraceContext context)
    {
        BeginCalls.Add((kind, context));
        return new RecordingScope(this);
    }

    private sealed class RecordingScope(RecordingJournalOperationTracer tracer) : IJournalOperationTraceScope
    {
        public void Dispose()
        {
        }

        public void SetFrameBytes(int payloadBytes) => tracer.FramePayloadBytes.Add(payloadBytes);
    }
}
