using System;

namespace Squirix.Server.Node.Services;

internal sealed class PersistedIdempotencyRecord
{
    public required DateTime CreatedUtc { get; init; }

    public required string Fingerprint { get; init; }

    public required string OperationId { get; init; }

    public required PersistedIdempotencyOutcome Outcome { get; init; }
}
