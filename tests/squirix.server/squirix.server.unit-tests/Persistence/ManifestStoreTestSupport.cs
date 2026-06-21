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
    internal static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
    };

    internal static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + Convert.ToInt64(timeout.TotalMilliseconds);
        while (!condition())
        {
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException("Timed out waiting for manifest retention side effects.");

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
    }

    internal static async Task<int> ReadCurrentManifestIndexAsync(string dataDir, CancellationToken cancellationToken)
    {
        var currentPath = Path.Combine(dataDir, $"{StorageFilePrefixes.Manifest}current");
        var pointerBytes = await File.ReadAllBytesAsync(currentPath, cancellationToken).ConfigureAwait(false);
        return ManifestPointer.Read(pointerBytes);
    }

    internal static string ManifestDataFileName(int index) =>
        $"{StorageFilePrefixes.Manifest}{index.ToString("D6", CultureInfo.InvariantCulture)}{StorageFileExtensions.Manifest}";
}
