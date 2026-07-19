namespace Squirix.Server.Storage.Journaling.Abstractions;

internal interface IStorageFileOperations
{
    bool PublishSnapshot(string tempPath, string finalPath);

    bool TryDelete(string path);
}
