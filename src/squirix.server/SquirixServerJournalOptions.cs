using System;
using System.Text.Json.Serialization;
using Squirix.Server.Storage;

namespace Squirix.Server;

/// <summary>Configures the node journal. Defaults leave journal group commit off: every durable mutation flushes the journal on its own.</summary>
/// <remarks>
/// Group commit lets concurrent durable mutations share one journal flush at the cost of up to <see cref="GroupCommitMaxWait" />
/// of extra commit latency. It requires persistence (see <see cref="SquirixServerOptions.UsePersistence" />).
/// </remarks>
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class SquirixServerJournalOptions
{
    /// <summary>
    /// Gets or sets the number of waiting durable mutations at which the journal flushes without waiting for <see cref="GroupCommitMaxWait" />;
    /// one flush may cover more. Default is <c language="csharp">32</c>; must be between 1 and 4096. Used only when <see cref="GroupCommitMaxWait" /> is greater than zero.
    /// </summary>
    public int GroupCommitMaxBatch { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum time a durable mutation waits for others before the journal issues one shared flush (group commit).
    /// Default is <see cref="TimeSpan.Zero" />: group commit is off and every durable mutation flushes on its own.
    /// </summary>
    /// <remarks>
    /// Must be zero or between 1 and 100 whole milliseconds ("00:00:00.002" in settings). A value greater than zero requires
    /// <see cref="SquirixServerOptions.PersistenceEnabled" />.
    /// </remarks>
    public TimeSpan GroupCommitMaxWait { get; set; } = TimeSpan.Zero;

    internal PersistenceOptions ToPersistenceOptions() => new()
    {
        JournalGroupCommitMaxBatch = GroupCommitMaxBatch,
        JournalGroupCommitMaxWait = GroupCommitMaxWait,
    };
}
