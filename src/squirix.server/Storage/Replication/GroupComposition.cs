using System;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace Squirix.Server.Storage.Replication;

/// <summary>Immutable set of replica groups the local node participates in.</summary>
/// <remarks>
/// A group directory is created only for a group present in this composition. This is the static local
/// membership check performed before any group storage is materialized on disk.
/// </remarks>
internal sealed class GroupComposition
{
    private readonly FrozenDictionary<string, byte> _groups;

    private GroupComposition(FrozenDictionary<string, byte> groups)
    {
        _groups = groups;
    }

    /// <summary>Gets the group identifiers in this composition.</summary>
    internal IEnumerable<string> GroupIds => _groups.Keys;

    // If a variable-length composition is ever required, add an overload accepting IReadOnlyList<string> (e.g., a List<string>)
    // and build the frozen dictionary in one pass.

    /// <summary>Creates a composition over a single replica group.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns>The immutable composition.</returns>
    /// <exception cref="ArgumentException">Thrown when the group identifier is null or whitespace.</exception>
    internal static GroupComposition Create(string groupId)
    {
        if (string.IsNullOrWhiteSpace(groupId))
            throw new ArgumentException("Group identifiers must not be null or whitespace.", nameof(groupId));

        return new GroupComposition(new[] { new KeyValuePair<string, byte>(groupId, 0) }.ToFrozenDictionary(StringComparer.Ordinal));
    }

    /// <summary>Creates a composition over exactly two replica groups.</summary>
    /// <param name="first">The first replica group identifier.</param>
    /// <param name="second">The second replica group identifier.</param>
    /// <returns>The immutable composition.</returns>
    /// <exception cref="ArgumentException">Thrown when a group identifier is null or whitespace.</exception>
    internal static GroupComposition Create(string first, string second)
    {
        if (string.IsNullOrWhiteSpace(first))
            throw new ArgumentException("Group identifiers must not be null or whitespace.", nameof(first));

        if (string.IsNullOrWhiteSpace(second))
            throw new ArgumentException("Group identifiers must not be null or whitespace.", nameof(second));

        if (string.Equals(first, second, StringComparison.Ordinal))
            throw new ArgumentException("Group identifiers must be unique; the composition already contains the group.", nameof(second));

        return new GroupComposition(new[] { new KeyValuePair<string, byte>(first, 0), new KeyValuePair<string, byte>(second, 0) }.ToFrozenDictionary(StringComparer.Ordinal));
    }

    /// <summary>Creates an empty composition.</summary>
    /// <returns>An empty composition.</returns>
    internal static GroupComposition Empty() => new(FrozenDictionary<string, byte>.Empty);

    /// <summary>Determines whether <paramref name="groupId" /> is part of this composition.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns><see langword="true" /> when the group is a member of this composition.</returns>
    internal bool Contains(string groupId) => _groups.ContainsKey(groupId);
}
