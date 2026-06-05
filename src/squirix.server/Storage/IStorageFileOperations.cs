namespace Squirix.Server.Storage;

internal interface IStorageFileOperations
{
    void PublishSnapshot(string tempPath, string finalPath);

    bool TryDelete(string path);
}
