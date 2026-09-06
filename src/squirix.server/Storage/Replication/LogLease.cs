using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>A reference-counted handle on a follower log that defers disposal until released.</summary>
[Mutable]
internal sealed class LogLease : IAsyncDisposable
{
    private readonly GroupRecovery _owner;
    private int _released;

    internal LogLease(IFollowerLog log, GroupRecovery owner)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(owner);
        Log = log;
        _owner = owner;
    }

    /// <summary>Gets the leased follower log.</summary>
    /// <returns>The leased follower log.</returns>
    internal IFollowerLog Log { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _released, 1) != 0)
            return;

        await _owner.ReleaseAsync(Log).ConfigureAwait(false);
    }
}
