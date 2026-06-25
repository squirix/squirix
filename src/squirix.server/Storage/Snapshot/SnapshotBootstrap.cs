using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Bootstrap;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>
/// Loads snapshot trigger settings from <c>Squirix.settings.json</c>.
/// </summary>
internal static class SnapshotBootstrap
{
    /// <summary>Loads snapshot trigger settings using the same settings file discovery as cluster bootstrap.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Loaded snapshot trigger options.</returns>
    public static async Task<SnapshotTriggerOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        var baseline = new SnapshotTriggerOptions();
        var (_, fileMerged) = await UnifiedSettings.TryMergeSnapshotFromFileAsync(baseline, cancellationToken).ConfigureAwait(false);
        return fileMerged;
    }
}
