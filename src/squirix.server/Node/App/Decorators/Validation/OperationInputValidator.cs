using System;

namespace Squirix.Server.Node.App.Decorators.Validation;

/// <summary>
/// Validates single-operation payloads such as cache entries and non-null factory delegates.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal static class OperationInputValidator<T>
{
    /// <summary>
    /// Validates a cache entry reference and its tags when present.
    /// </summary>
    /// <param name="entry">The entry to validate.</param>
    public static void ValidateEntry(CacheEntry<T>? entry) => ArgumentNullException.ThrowIfNull(entry);
}
