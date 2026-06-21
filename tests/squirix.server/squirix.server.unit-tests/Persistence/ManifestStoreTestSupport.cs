using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest.Binary;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Helpers for manifest store tests across JSON and binary backends.</summary>
internal static class ManifestStoreTestSupport
{
    internal static PersistenceOptions CreateOptions(string dataDir, ManifestBackend backend) => new()
    {
        DataDir = dataDir,
        ManifestBackend = backend,
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

    internal static async Task<int> ReadCurrentManifestIndexAsync(string dataDir, ManifestBackend backend, CancellationToken cancellationToken)
    {
        var currentPath = Path.Combine(dataDir, $"{StorageFilePrefixes.Manifest}current");
        if (backend is ManifestBackend.Binary)
        {
            var pointerBytes = await File.ReadAllBytesAsync(currentPath, cancellationToken).ConfigureAwait(false);
            return BinaryManifestPointer.Read(pointerBytes);
        }

        var name = (await File.ReadAllTextAsync(currentPath, cancellationToken).ConfigureAwait(false)).Trim();
        var prefix = StorageFilePrefixes.Manifest;
        var suffix = StorageFileExtensions.Manifest;
        var numberPart = name.Substring(prefix.Length, name.Length - prefix.Length - suffix.Length);
        return int.Parse(numberPart, CultureInfo.InvariantCulture);
    }

    internal static string ManifestDataFileName(int index, ManifestBackend backend) =>
        $"{StorageFilePrefixes.Manifest}{index.ToString("D6", CultureInfo.InvariantCulture)}{ManifestExtension(backend)}";

    private static string ManifestExtension(ManifestBackend backend) =>
        backend is ManifestBackend.Binary ? StorageFileExtensions.BinaryManifest : StorageFileExtensions.Manifest;
}
