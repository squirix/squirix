using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Bootstrap;

/// <summary>Loads snapshot trigger settings from <c>Squirix.settings.json</c>.</summary>
internal static class SnapshotBootstrap
{
    /// <summary>Loads snapshot trigger settings using the same settings file discovery as cluster bootstrap.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Loaded snapshot trigger options.</returns>
    internal static async Task<TriggerOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        var baseline = new TriggerOptions();
        var (_, fileMerged) = await UnifiedSettings.TryMergeSnapshotFromFileAsync(baseline, cancellationToken).ConfigureAwait(false);
        return fileMerged;
    }
}
