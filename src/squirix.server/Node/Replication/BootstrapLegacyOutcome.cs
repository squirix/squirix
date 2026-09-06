using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Replication;

/// <summary>Validated legacy idempotency outcome discovered in RF=1 source state.</summary>
/// <param name="OperationId">Opaque legacy operation identity.</param>
/// <param name="HasReplicaGroupScope">Whether the outcome already carries an unambiguous replica group.</param>
/// <param name="ExpiresUtc">Retention expiration, when known.</param>
[Immutable]
internal sealed record BootstrapLegacyOutcome(string OperationId, bool HasReplicaGroupScope, DateTimeOffset? ExpiresUtc);
