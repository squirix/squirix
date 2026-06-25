using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Helpers for manifest store tests.</summary>
internal static class ManifestStoreTestSupport
{
    internal const string JournalSegment000001 = $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}";
    internal const string JournalSegment000002 = $"{StorageFilePrefixes.Journal}000002{StorageFileExtensions.Journal}";
    internal const string JournalSegment000003 = $"{StorageFilePrefixes.Journal}000003{StorageFileExtensions.Journal}";
    internal const string Snapshot000001 = $"{StorageFilePrefixes.Snapshot}000001{StorageFileExtensions.Snapshot}";
    internal const string Snapshot000002 = $"{StorageFilePrefixes.Snapshot}000002{StorageFileExtensions.Snapshot}";
    internal const string Manifest000001 = $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}";
    internal const string Manifest000003 = $"{StorageFilePrefixes.Manifest}000003{StorageFileExtensions.Manifest}";
    internal const string ManifestCurrentPointer = $"{StorageFilePrefixes.Manifest}current";

    internal static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
    };

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

    internal static async Task<int> ReadCurrentManifestIndexAsync(string dataDir, CancellationToken cancellationToken)
    {
        var currentPath = Path.Join(dataDir, ManifestCurrentPointer);
        var pointerBytes = await File.ReadAllBytesAsync(currentPath, cancellationToken).ConfigureAwait(false);
        return ManifestPointer.Read(pointerBytes);
    }

    internal static string ManifestDataFileName(int index) =>
        $"{StorageFilePrefixes.Manifest}{index.ToString("D6", CultureInfo.InvariantCulture)}{StorageFileExtensions.Manifest}";
}
