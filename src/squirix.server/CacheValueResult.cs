namespace Squirix.Server;

/// <summary>
/// Outcome of a cache lookup that returns a typed value. Provides a <see cref="bool" /> flag
/// to disambiguate between "not found" and a stored <see langword="null"/> value.
/// </summary>
/// <param name="Found">Indicates whether the key was present and not expired.</param>
/// <param name="Value">The retrieved value (may be <see langword="null"/>).</param>
/// <typeparam name="T">The cache value type.</typeparam>
public readonly record struct CacheValueResult<T>(bool Found, T? Value);
