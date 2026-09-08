using System;

namespace Squirix.Server.Node.Observability;

/// <summary>Compares durable replica identity against the configured topology without mutating state.</summary>
/// <remarks>
/// A group with no durable progress carries an empty fingerprint and generation zero: there is nothing
/// to disagree about, so both compare as matching. Any durable identity must match exactly.
/// </remarks>
internal static class ReplicaTopologyMatch
{
    /// <summary>Compares a durable configuration generation against the configured generation.</summary>
    /// <param name="stored">The durable configuration generation.</param>
    /// <param name="expected">The configured configuration generation.</param>
    /// <returns><see langword="true" /> when the group is uninitialized or the generations agree.</returns>
    internal static bool MatchesGeneration(ulong stored, ulong expected) => stored == 0UL || stored == expected;

    /// <summary>Compares a durable topology fingerprint against the configured fingerprint.</summary>
    /// <param name="stored">The durable topology fingerprint.</param>
    /// <param name="expected">The configured topology fingerprint.</param>
    /// <returns><see langword="true" /> when the group is uninitialized or the fingerprints agree.</returns>
    internal static bool MatchesFingerprint(ReadOnlyMemory<byte> stored, ReadOnlySpan<byte> expected) => stored.IsEmpty || stored.Span.SequenceEqual(expected);
}
