using System;

namespace Squirix.Core;

/// <summary>
/// Canonical logical cache name for routing, journal namespaces, scan, watch, and tag invalidation after public validation.
/// </summary>
internal readonly struct CacheName : IEquatable<CacheName>
{
    private CacheName(string canonical)
    {
        Canonical = canonical;
    }

    /// <summary>
    /// Gets the canonical string used consistently across routing, persistence keys, and observability.
    /// </summary>
    public string Canonical { get; }

    public static bool operator ==(CacheName left, CacheName right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(CacheName left, CacheName right)
    {
        return !left.Equals(right);
    }

    /// <summary>
    /// Maps null, empty, or whitespace-only names to <see cref="CacheNames.DefaultNamespace" /> without applying public validation.
    /// </summary>
    /// <param name="cacheName">Logical name from an already-validated pipeline segment or trusted persistence.</param>
    /// <returns>The canonical cache name string for routing.</returns>
    public static string NormalizeUnvalidated(string? cacheName) => string.IsNullOrWhiteSpace(cacheName) ? CacheNames.DefaultNamespace : cacheName;

    /// <summary>
    /// Validates <paramref name="name" /> using public cache name rules and returns the canonical runtime value.
    /// </summary>
    /// <param name="name">Logical cache name from a public or wire boundary.</param>
    /// <param name="parameterName">Caller parameter name for exceptions.</param>
    /// <returns>A <see cref="CacheName" /> whose <see cref="Canonical" /> is safe for the internal pipeline.</returns>
    public static CacheName ParsePublic(string? name, string parameterName = "cacheName")
    {
        var validated = CacheNameValidator.Validate(name, parameterName);
        return new CacheName(NormalizeUnvalidated(validated));
    }

    public override bool Equals(object? obj) => obj is CacheName other && Equals(other);

    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(Canonical);

    public override string ToString() => Canonical;

    public bool Equals(CacheName other) => string.Equals(Canonical, other.Canonical, StringComparison.Ordinal);
}
