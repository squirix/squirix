using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Opens group copies and seeds them the way the first start of a group does, so a test log takes entries, grants votes and leads.</summary>
/// <remarks>A fresh copy is unseeded and refuses all of that; production seeds the copy of the owner with the step used here.</remarks>
internal static class SeededLogs
{
    /// <summary>Opens a log and completes its copy when it is unseeded, like the owner does at the first start of its group.</summary>
    /// <param name="log">The log to open.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the log is open and its copy is complete.</returns>
    internal static async Task OpenSeededAsync(this FollowerLog log, CancellationToken cancellationToken)
    {
        await log.OpenAsync(cancellationToken).ConfigureAwait(false);
        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (status.MemberState == GroupMemberState.Unseeded)
            await log.CompleteBootstrapAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Seeds the groups on a data directory, so a registry opened on it afterwards finds complete copies.</summary>
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
