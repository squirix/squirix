using System.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

internal sealed class StorageFileOperations : IStorageFileOperations
{
    public void PublishSnapshot(string tempPath, string finalPath)
    {
        if (File.Exists(finalPath))
        {
            File.Replace(tempPath, finalPath, null, true);
            return;
        }

        File.Move(tempPath, finalPath);
    }

    public bool TryDelete(string path) => FileEx.TryDeleteFile(path);
}
