using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Storage;

/// <summary>Thin facade over <see cref="Manifest.IManifestStore" /> implementations selected by <see cref="ManifestBackend" />.</summary>
internal sealed class ManifestStore : IDisposable
{
    private readonly Manifest.IManifestStore _inner;

    public ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger = null, IRetentionCleanupReadinessStatus? retentionReadiness = null)
        : this(Manifest.ManifestStoreFactory.Create(options, logger, retentionReadiness))
    {
    }

    internal ManifestStore(PersistenceOptions options, ILogger<ManifestStore>? logger, IRetentionCleanupReadinessStatus? retentionReadiness, IStorageFileOperations fileOperations)
        : this(Manifest.ManifestStoreFactory.Create(options, logger, retentionReadiness, fileOperations))
    {
    }

    private ManifestStore(Manifest.IManifestStore inner)
    {
        _inner = inner;
    }

    public Task<Manifest.ManifestState> ReadCurrentOrDefaultAsync(CancellationToken cancellationToken = default) => _inner.ReadCurrentOrDefaultAsync(cancellationToken);

    public Manifest.ManifestState ReadCurrentOrDefaultBlocking() => _inner.ReadCurrentOrDefaultBlocking();

    public Task WriteAsync(Manifest.ManifestState manifest, CancellationToken cancellationToken = default) => _inner.WriteAsync(manifest, cancellationToken);

    public void Dispose() => _inner.Dispose();

    internal void PublishBlocking(Manifest.ManifestState manifest) => _inner.PublishBlocking(manifest);

    internal void PublishRollBlocking(int currentJournal, ulong nextSequence) => _inner.PublishRollBlocking(currentJournal, nextSequence);
}
