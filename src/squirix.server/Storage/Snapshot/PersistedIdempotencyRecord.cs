using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Durable idempotency outcome captured in node snapshots.</summary>
[Immutable]
internal sealed class PersistedIdempotencyRecord
{
    /// <summary>Initializes a new instance of the <see cref="PersistedIdempotencyRecord" /> class.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The mutation fingerprint.</param>
    /// <param name="responseBytes">The recorded response bytes.</param>
    /// <param name="createdUtc">The record creation timestamp.</param>
    internal PersistedIdempotencyRecord(string operationId, string fingerprint, byte[] responseBytes, DateTime createdUtc)
        : this(operationId, fingerprint, responseBytes, IdempotencyRecordState.Completed, createdUtc)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PersistedIdempotencyRecord" /> class for a write-ahead started record.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The mutation fingerprint when known; otherwise <see langword="null" /> for records reconstructed from journal mutation frames.</param>
    /// <param name="createdUtc">The record creation timestamp.</param>
    internal PersistedIdempotencyRecord(string operationId, string? fingerprint, DateTime createdUtc)
        : this(operationId, fingerprint, [], IdempotencyRecordState.Started, createdUtc)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PersistedIdempotencyRecord" /> class.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="fingerprint">The mutation fingerprint when known; otherwise <see langword="null" />.</param>
    /// <param name="responseBytes">The recorded response bytes; empty for started records.</param>
    /// <param name="state">The record lifecycle state.</param>
    /// <param name="createdUtc">The record creation timestamp.</param>
    private PersistedIdempotencyRecord(string operationId, string? fingerprint, byte[] responseBytes, IdempotencyRecordState state, DateTime createdUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentNullException.ThrowIfNull(responseBytes);

        OperationId = operationId;
        Fingerprint = fingerprint;
        ResponseBytes = responseBytes;
        State = state;
        CreatedUtc = createdUtc;
    }

    internal DateTime CreatedUtc { get; }

    internal string? Fingerprint { get; }

    internal string OperationId { get; }

    internal byte[] ResponseBytes { get; }

    internal IdempotencyRecordState State { get; }
}
