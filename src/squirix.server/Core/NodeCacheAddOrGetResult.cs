using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>The add outcome and, when nothing was added, the existing value read by GetOrAdd.</summary>
/// <param name="Added">Whether this operation added the requested entry, including replay of its recorded add.</param>
/// <param name="Existing">The value read when the add did not take effect; a miss when the key disappeared.</param>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal readonly record struct NodeCacheAddOrGetResult<T>(bool Added, NodeCacheValueResult<T> Existing);
