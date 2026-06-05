namespace Squirix;

/// <summary>
/// Outcome of a cache value lookup.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <param name="Found">Indicates whether the key was present and not expired.</param>
/// <param name="Value">The retrieved value, which may be <c>null</c> when <paramref name="Found" /> is <c>true</c>.</param>
public readonly record struct CacheValueResult<T>(bool Found, T? Value);
