using Squirix.Server.Attributes;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Identifies a mutating cache operation for decorator pipeline state.</summary>
/// <param name="OperationId">Operation id carried for tracing and idempotency.</param>
/// <param name="CacheName">Logical cache name.</param>
/// <param name="Key">Cache key.</param>
[Immutable]
internal readonly record struct MutationKeyArgs(string OperationId, string CacheName, string Key);
