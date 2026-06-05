namespace Squirix.Server;

/// <summary>
/// Outcome of a cache removal that returns the removed value when successful.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal readonly struct CacheRemoveResult<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CacheRemoveResult{T}" /> struct.
    /// </summary>
    /// <param name="removed">Indicates whether the key existed and was removed.</param>
    /// <param name="value">The value that was removed (may be <c>null</c>).</param>
    public CacheRemoveResult(bool removed, T? value)
    {
        Removed = removed;
        Value = value;
    }

    /// <summary>
    /// Gets a value indicating whether the key existed and the entry was removed.
    /// </summary>
    public bool Removed { get; }

    /// <summary>
    /// Gets the value that was removed when <see cref="Removed" /> is <c>true</c>. This may be <c>null</c> if the entry stored a <c>null</c> value.
    /// </summary>
    /// <remarks>
    /// When <see cref="Removed" /> is <c>false</c> (key was not present), <see cref="Value" /> is the default of <c>T</c> and must not be used as a removed payload.
    /// </remarks>
    public T? Value { get; }
}
