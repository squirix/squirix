using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Opens group copies that hold a complete copy of their group, so a test log takes entries, grants votes and leads.</summary>
/// <remarks>The one place that decides how a test gets such a copy: a fresh copy opens complete, so opening it is all it takes.</remarks>
internal static class SeededLogs
{
    /// <summary>Opens a log whose copy is complete.</summary>
    /// <param name="log">The log to open.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the log is open and its copy is complete.</returns>
    internal static Task OpenSeededAsync(this FollowerLog log, CancellationToken cancellationToken) => log.OpenAsync(cancellationToken);

    /// <summary>Creates the copies of the groups on a data directory, so a registry opened on it afterwards finds complete copies.</summary>
    /// <param name="root">The node data directory.</param>
    /// <param name="groupIds">The groups to seed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every group holds a complete copy.</returns>
    internal static async Task SeedGroupsAsync(string root, IReadOnlyList<string> groupIds, CancellationToken cancellationToken)
    {
        var composition = GroupComposition.Create(groupIds);
        for (var i = 0; i < groupIds.Count; i++)
        {
            await using var log = new FollowerLog(root, groupIds[i], composition, NullLogger<FollowerLog>.Instance, TimeProvider.System);
            await log.OpenSeededAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
