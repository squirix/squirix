using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Durable idempotency outcome captured in node snapshots.</summary>
[Immutable]
internal sealed class PersistedIdempotencyRecord
{
    internal PersistedIdempotencyRecord(string operationId, string fingerprint, byte[] responseBytes, DateTime createdUtc)
    {
        OperationId = operationId;
        Fingerprint = fingerprint;
        ResponseBytes = responseBytes;
        CreatedUtc = createdUtc;
    }

    internal DateTime CreatedUtc { get; }

    internal string Fingerprint { get; }

    internal string OperationId { get; }

    internal byte[] ResponseBytes { get; }
}
