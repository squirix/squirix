using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Carries a touch-expiration cache operation for decorator pipeline state.</summary>
/// <param name="OperationId">Operation id carried for tracing and idempotency.</param>
/// <param name="CacheName">Logical cache name.</param>
/// <param name="Key">Cache key.</param>
/// <param name="Expiration">New expiration.</param>
[Immutable]
internal readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);
