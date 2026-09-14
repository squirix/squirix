using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Carries a set-entry cache operation for decorator pipeline state.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <param name="OperationId">Operation id carried for tracing and idempotency.</param>
/// <param name="CacheName">Logical cache name.</param>
/// <param name="Key">Cache key.</param>
/// <param name="Entry">Entry to store.</param>
[Immutable]
internal readonly record struct SetEntryArgs<T>(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry);
