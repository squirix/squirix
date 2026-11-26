using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>
/// Outcome of a cache lookup that returns a typed value. Provides a <see cref="bool" /> flag
/// to disambiguate between "not found" and a stored <see langword="null" /> value.
/// </summary>
/// <param name="Found">Indicates whether the key was present and not expired.</param>
/// <param name="Value">The retrieved value (may be <see langword="null" />).</param>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
public readonly record struct NodeCacheValueResult<T>(bool Found, T? Value)
{
    /// <inheritdoc />
    public bool Equals(NodeCacheValueResult<T> other) => Found == other.Found && System.Collections.Generic.EqualityComparer<T?>.Default.Equals(Value, other.Value);

    /// <inheritdoc />
    public override int GetHashCode() => System.HashCode.Combine(Found, Value);
}
