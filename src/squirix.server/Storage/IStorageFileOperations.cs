namespace Squirix.Server.Storage;

internal interface IStorageFileOperations
{
    bool PublishSnapshot(string tempPath, string finalPath);

    bool TryDelete(string path);
}
