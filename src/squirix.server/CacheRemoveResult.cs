namespace Squirix.Server;

/// <summary>Outcome of a cache removal that returns the removed value when successful.</summary>
/// <param name="Removed">Indicates whether the key existed and was removed.</param>
/// <param name="Value">The value that was removed (may be <see langword="null"/>).</param>
/// <typeparam name="T">The cache value type.</typeparam>
public readonly record struct CacheRemoveResult<T>(bool Removed, T? Value);
