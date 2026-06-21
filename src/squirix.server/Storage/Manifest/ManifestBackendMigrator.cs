using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Manifest.Binary;
using Squirix.Server.Storage.Manifest.Json;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>One-shot migration from JSON manifests to the binary manifest backend.</summary>
[SuppressMessage("Design", "MA0182:Unused internal type", Justification = "Invoked by unit tests and operational migration workflows.")]
internal static class ManifestBackendMigrator
{
    /// <summary>Reads the latest JSON manifest and writes the first binary manifest plus SQMC pointer.</summary>
    /// <param name="options">Persistence options including the data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when migration finishes.</returns>
    public static async Task MigrateJsonToBinaryAsync(PersistenceOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var dataDir = options.DataDir;
        if (string.IsNullOrWhiteSpace(dataDir))
            throw new InvalidOperationException("Persistence DataDir is required for manifest migration.");

        _ = await DirectoryEx.CreateDirectoryAsync(dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);

        var currentPath = PathEx.Combine(dataDir, $"{StorageFilePrefixes.Manifest}current");
        if (!File.Exists(currentPath))
            return;

        var pointerBytes = await File.ReadAllBytesAsync(currentPath, cancellationToken).ConfigureAwait(false);
        if (BinaryManifestPointer.IsBinaryPointer(pointerBytes))
            return;

        var pointerText = Encoding.UTF8.GetString(pointerBytes).Trim();
        if (string.IsNullOrWhiteSpace(pointerText))
            throw new InvalidDataException($"Manifest current pointer is empty: {currentPath}");

        var jsonPath = PathEx.Combine(dataDir, pointerText);
        if (!File.Exists(jsonPath))
            throw new FileNotFoundException($"JSON manifest referenced by CURRENT was not found: {jsonPath}", jsonPath);

        var jsonStore = new JsonManifestStore(options);
        var manifest = await jsonStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        jsonStore.Dispose();

        var manifestIndex = TryParseJsonManifestIndex(pointerText);
        if (manifestIndex <= 0)
            manifestIndex = 1;

        var binaryStore = new BinaryManifestStore(options);
        try
        {
            await binaryStore.WriteMigrationInitialAsync(manifest, manifestIndex, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            binaryStore.Dispose();
        }
    }

    private static int TryParseJsonManifestIndex(string name)
    {
        if (string.IsNullOrEmpty(name))
            return 0;
        if (!name.StartsWith(StorageFilePrefixes.Manifest, StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!name.EndsWith(StorageFileExtensions.Manifest, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Substring(
            StorageFilePrefixes.Manifest.Length,
            name.Length - StorageFilePrefixes.Manifest.Length - StorageFileExtensions.Manifest.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }
}
