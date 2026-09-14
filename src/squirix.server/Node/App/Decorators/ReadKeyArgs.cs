using Squirix.Server.Attributes;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Identifies a read cache operation for decorator pipeline state.</summary>
/// <param name="CacheName">Logical cache name.</param>
/// <param name="Key">Cache key.</param>
[Immutable]
internal readonly record struct ReadKeyArgs(string CacheName, string Key);
