namespace Squirix.Server;

/// <summary>
/// Outcome of a cache lookup that returns a typed value. Provides a <see cref="bool" /> flag
/// to disambiguate between "not found" and a stored <c>null</c> value.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal readonly struct CacheValueResult<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CacheValueResult{T}" /> struct.
    /// </summary>
    /// <param name="found">Indicates whether the key was present and not expired.</param>
    /// <param name="value">The retrieved value (may be <c>null</c>).</param>
    public CacheValueResult(bool found, T? value)
    {
        Found = found;
        Value = value;
    }

    /// <summary>
    /// Gets a value indicating whether the key was found.
    /// </summary>
    public bool Found { get; }

    /// <summary>
    /// Gets the stored value when <see cref="Found" /> is <c>true</c>. This may be <c>null</c> when a present entry explicitly stores a <c>null</c> value.
    /// </summary>
    /// <remarks>
    /// When <see cref="Found" /> is <c>false</c> (key missing or expired), <see cref="Value" /> is the default of <c>T</c> and must not be used to infer storage.
    /// </remarks>
    public T? Value { get; }
}
