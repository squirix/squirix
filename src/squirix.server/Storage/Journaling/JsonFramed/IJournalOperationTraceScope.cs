using System;

namespace Squirix.Server.Storage.Journaling.JsonFramed;

/// <summary>Active trace scope for a journal operation.</summary>
internal interface IJournalOperationTraceScope : IDisposable
{
    /// <summary>Records serialized journal frame size tags on the active span.</summary>
    /// <param name="payloadBytes">Payload byte length excluding framing.</param>
    void SetFrameBytes(int payloadBytes);
}
