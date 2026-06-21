using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Manifest;

internal interface IManifestStore : System.IDisposable
{
    Task<ManifestState> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default);

    Task WriteAsync(ManifestState manifest, CancellationToken cancellationToken = default);

    ManifestState ReadCurrentOrDefaultBlocking();

    void WriteBlocking(ManifestState manifest);

    void PublishBlocking(ManifestState manifest);

    void PublishRollBlocking(int currentJournal, ulong nextSequence);
}
