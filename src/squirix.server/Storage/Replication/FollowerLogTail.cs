using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>The fixed index bounds and suffix revision captured at leader startup.</summary>
/// <param name="Status">The paired commit and last-log bounds.</param>
/// <param name="Revision">Changes whenever the journal replaces its suffix.</param>
[Immutable]
internal sealed record FollowerLogTail(FollowerLogStatus Status, long Revision)
{
    internal const int BatchEntries = 256;
    internal const long BatchBytes = 4L * 1024 * 1024;

    internal static int CountThrough(ulong afterIndex, ulong throughIndex) =>
        int.CreateChecked(System.Math.Min(ulong.CreateChecked(BatchEntries), throughIndex - afterIndex));
}
