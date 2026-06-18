using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Squirix.E2ETests.Support.Stress;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Support.Restart;

internal sealed class RestartableSingleNode : IAsyncDisposable
{
    private readonly TempDirectory? _ownedDataDir;
    private ISquirixClient? _client;
    private TestNodeHost? _host;

    private RestartableSingleNode(TempDirectory? ownedDataDir, string dataDir, Uri uri, TestNodeHostStartOptions startOptions, TimeSpan? clientRpcPerAttemptTimeout)
    {
        _ownedDataDir = ownedDataDir;
        DataDir = dataDir;
        Uri = uri;
        StartOptions = startOptions;
        ClientRpcPerAttemptTimeout = clientRpcPerAttemptTimeout;
    }

    /// <summary>Gets the persistence data directory for the node.</summary>
    internal string DataDir { get; }

    private Uri Uri { get; }

    private TestNodeHostStartOptions StartOptions { get; }

    private TimeSpan? ClientRpcPerAttemptTimeout { get; }

    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        await StopNodeAsync().ConfigureAwait(false);
        _ownedDataDir?.Dispose();
    }

    internal static ValueTask<RestartableSingleNode> StartAsync(string testName, CancellationToken cancellationToken) =>
        StartWithOptionsAsync(testName, new TestNodeHostStartOptions(), null, cancellationToken);

    internal static async ValueTask<RestartableSingleNode> StartWithOptionsAsync(
        string testName,
        TestNodeHostStartOptions hostOptions,
        TimeSpan? clientRpcPerAttemptTimeout,
        CancellationToken cancellationToken)
    {
        // A fixed persistence directory (host override or the stress env var) is preserved for post-failure
        // inspection, so it is used as-is rather than wrapped in a self-deleting TempDirectory.
        var persistDirOverride = hostOptions.DataDir ?? JournalVolumeStressSettings.PersistDir;
        TempDirectory? ownedDataDir = null;
        string dataDirPath;
        if (persistDirOverride is null)
        {
            ownedDataDir = new TempDirectory("squirix-e2e-restartable", testName);
            dataDirPath = ownedDataDir.Path;
        }
        else
        {
            dataDirPath = persistDirOverride;
            _ = Directory.CreateDirectory(dataDirPath);
        }

        var effectiveOptions = new TestNodeHostStartOptions
        {
            DataDir = dataDirPath,
            JournalMaxSegmentMb = hostOptions.JournalMaxSegmentMb,
            JournalMaxSegmentCount = hostOptions.JournalMaxSegmentCount,
            JournalMaxTotalBytesMb = hostOptions.JournalMaxTotalBytesMb,
            FlushIntervalMs = hostOptions.FlushIntervalMs,
            SnapshotInterval = hostOptions.SnapshotInterval,
            JournalGroupCommitMaxWaitMs = hostOptions.JournalGroupCommitMaxWaitMs,
            Security = hostOptions.Security,
            MtlsProfile = hostOptions.MtlsProfile,
        };

        var node = new RestartableSingleNode(ownedDataDir, dataDirPath, ListenPortPool.EndToEndTests.NextHttpUri(), effectiveOptions, clientRpcPerAttemptTimeout);
        await node.StartNodeAsync(cancellationToken);
        return node;
    }

    internal async ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken)
    {
        _client ??= await LoopbackConnect.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(Uri);
                if (ClientRpcPerAttemptTimeout is { } rpcTimeout)
                    options.RpcPerAttemptTimeout = rpcTimeout;
            },
            cancellationToken);
        return await _client.GetCacheAsync<T>(cacheName, cancellationToken);
    }

    internal async ValueTask RestartAsync(CancellationToken cancellationToken)
    {
        await StopNodeAsync();
        await StartNodeAsync(cancellationToken);
    }

    private async ValueTask StartNodeAsync(CancellationToken cancellationToken)
    {
        var topology = new[] { ("nodeA", Uri) };
        _host = await TestNodeHostFactory.StartNodeAsync("nodeA", Uri, topology, StartOptions, null, cancellationToken);
    }

    private async ValueTask StopNodeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync();
            _client = null;
        }

        if (_host is not null)
        {
            await _host.DisposeAsync();
            _host = null;
        }
    }
}
