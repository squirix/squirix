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

    /// <summary>Creates an empty composition.</summary>
    /// <returns>An empty composition.</returns>
    internal static GroupComposition Empty() => new(FrozenDictionary<string, byte>.Empty);

    /// <summary>Creates a composition over the supplied group identifiers.</summary>
    /// <param name="groupIds">The replica group identifiers in the composition.</param>
    /// <returns>The immutable composition.</returns>
    /// <exception cref="ArgumentException">Thrown when a group identifier is null or whitespace.</exception>
    internal static GroupComposition Create(IEnumerable<string> groupIds)
    {
        ArgumentNullException.ThrowIfNull(groupIds);
        var builder = new Dictionary<string, byte>(StringComparer.Ordinal);
        foreach (var groupId in groupIds)
        {
            if (string.IsNullOrWhiteSpace(groupId))
                throw new ArgumentException("Group identifiers must not be null or whitespace.", nameof(groupIds));

            builder[groupId] = 0;
        }

        return new GroupComposition(builder.ToFrozenDictionary(StringComparer.Ordinal));
    }

    /// <summary>Determines whether <paramref name="groupId" /> is part of this composition.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <returns><see langword="true" /> when the group is a member of this composition.</returns>
    internal bool Contains(string groupId) => _groups.ContainsKey(groupId);
}
