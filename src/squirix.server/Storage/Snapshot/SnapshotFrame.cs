using Squirix.Server.Node.Services;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class SnapshotFrame
{
    public object? Entry { get; init; }

    public PersistedIdempotencyRecord? Idempotency { get; init; }

    public string? Key { get; init; }

    public required string Kind { get; init; }

    public string? Namespace { get; init; }
}
