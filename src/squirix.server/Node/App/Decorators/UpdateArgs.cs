using Squirix.Server.Attributes;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Carries an update cache operation for decorator pipeline state.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <param name="OperationId">Operation id carried for tracing and idempotency.</param>
/// <param name="CacheName">Logical cache name.</param>
/// <param name="Key">Cache key.</param>
/// <param name="Value">New value.</param>
[Immutable]
internal readonly record struct UpdateArgs<T>(string OperationId, string CacheName, string Key, T? Value);
