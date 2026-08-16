using Squirix.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Abstractions;

[Immutable]
internal sealed class FileOperations : IStorageFileOperations
{
    public bool PublishSnapshot(string tempPath, string finalPath)
    {
        FileEx.PublishFile(tempPath, finalPath, ignoreMetadataErrors: true);
        return true;
    }

    public bool TryDelete(string path) => FileEx.TryDeleteFile(path);
}
