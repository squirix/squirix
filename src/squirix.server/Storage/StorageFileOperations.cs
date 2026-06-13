using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

internal sealed class StorageFileOperations : IStorageFileOperations
{
    public void PublishSnapshot(string tempPath, string finalPath) => FileEx.PublishFile(tempPath, finalPath, ignoreMetadataErrors: true);

    public bool TryDelete(string path) => FileEx.TryDeleteFile(path);
}
