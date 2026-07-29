using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Helpers for manifest store tests.</summary>
internal static class StoreTestSupport
{
    internal const string JournalSegment000001 = $"{FilePrefixes.Journal}000001{FileExtensions.Journal}";
    internal const string JournalSegment000002 = $"{FilePrefixes.Journal}000002{FileExtensions.Journal}";
    internal const string JournalSegment000003 = $"{FilePrefixes.Journal}000003{FileExtensions.Journal}";
    internal const string Manifest000001 = $"{FilePrefixes.Manifest}000001{FileExtensions.Manifest}";
    internal const string Manifest000003 = $"{FilePrefixes.Manifest}000003{FileExtensions.Manifest}";
    internal const string ManifestCurrentPointer = $"{FilePrefixes.Manifest}current";
    internal const string Snapshot000001 = $"{FilePrefixes.Snapshot}000001{FileExtensions.Snapshot}";
    internal const string Snapshot000002 = $"{FilePrefixes.Snapshot}000002{FileExtensions.Snapshot}";

    internal static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
    };

    internal static string ManifestDataFileName(int index) => $"{FilePrefixes.Manifest}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Manifest}";

    internal static async Task<int> ReadCurrentManifestIndexAsync(string dataDir, CancellationToken cancellationToken)
    {
        var currentPath = Path.Join(dataDir, ManifestCurrentPointer);
        var pointerBytes = await File.ReadAllBytesAsync(currentPath, cancellationToken).ConfigureAwait(false);
        return Pointer.Read(pointerBytes);
    }

    internal static async Task WaitUntilAsync<T>(T state, Func<T, bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var deadline = Environment.TickCount64 + Convert.ToInt64(timeout.TotalMilliseconds);
        while (!condition(state))
        {
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException("Timed out waiting for manifest retention side effects.");

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
    }
}
