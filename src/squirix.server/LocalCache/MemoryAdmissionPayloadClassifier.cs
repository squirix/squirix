namespace Squirix.Server.LocalCache;

/// <summary>Mirrors <see cref="CacheEntrySizeEstimator{T}" /> payload classification for conservative admission decisions.</summary>
internal static class MemoryAdmissionPayloadClassifier
{
    /// <summary>Returns whether the typed payload maps to the estimator's conservative unknown bucket (non-counter path).</summary>
    /// <typeparam name="T">The cache value type.</typeparam>
    /// <param name="value">The value carried by a cache entry.</param>
    /// <returns><see langword="true" /> when the estimator would use an unknown typed payload fallback.</returns>
    internal static bool IsUnknownTypedPayloadEstimate<T>(T? value)
    {
        return value switch
        {
            null => false,
            string => false,
            byte[] => false,
            bool => false,
            char => false,
            sbyte => false,
            byte => false,
            short => false,
            ushort => false,
            int => false,
            uint => false,
            float => false,
            long => false,
            ulong => false,
            double => false,
            decimal => false,
            _ => true,
        };
    }
}
