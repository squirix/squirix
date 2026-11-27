using System;
using System.Text.Json.Serialization;
using Squirix.Server.Storage;

namespace Squirix.Server;

/// <summary>Configures the node journal. Persistent nodes use group commit for local journal durability.</summary>
/// <remarks>
/// Group commit lets concurrent durable mutations share one journal flush at the cost of up to <see cref="GroupCommitMaxWait" />
/// of extra commit latency. It requires persistence (see <see cref="SquirixServerOptions.UsePersistence" />).
/// </remarks>
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class SquirixServerJournalOptions
{
    /// <summary>
    /// Gets or sets the number of waiting durable mutations at which the journal flushes without waiting for <see cref="GroupCommitMaxWait" />;
    /// one flush may cover more. Default is <c language="csharp">32</c>; must be between 1 and 4096.
    /// </summary>
    public int GroupCommitMaxBatch { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum time a durable mutation waits for others before the journal issues one shared flush (group commit).
    /// Defaults to 2 milliseconds. Zero makes the group eligible to flush immediately.
    /// </summary>
    /// <remarks>Accepts 0 to 100 whole milliseconds. With zero wait, writers already waiting can still share a flush.</remarks>
    public TimeSpan GroupCommitMaxWait { get; set; } = TimeSpan.FromMilliseconds(2);

    internal PersistenceOptions ToPersistenceOptions() => new()
    {
        JournalGroupCommitMaxBatch = GroupCommitMaxBatch,
        JournalGroupCommitMaxWait = GroupCommitMaxWait,
    };
}
