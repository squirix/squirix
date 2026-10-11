using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of one group metadata publish next to a raw write and flush of one block, on a real temp directory.</summary>
/// <remarks>The cost of a single append with its flush and publish is <see cref="FollowerAppendBenchmarks.AppendSingleOrderedEntryAsync" />.</remarks>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class GroupMetadataPublishBenchmarks
{
    private const string GroupId = "grp-bench";
    private const int OperationsPerInvoke = 200;
    private const int BlockBytes = 4096;

    private readonly byte[] _block = new byte[BlockBytes];
    private TempDirectory? _dir;
    private SafeFileHandle? _handle;
    private FollowerLog? _log;
    private ulong _term;

    /// <summary>Disposes the log, the raw file handle, and the temp directory.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _handle?.Dispose();
        _handle = null;
        if (_log != null)
            await _log.DisposeAsync().ConfigureAwait(false);
        _log = null;
        _dir?.Dispose();
        _dir = null;
    }

    /// <summary>Observes a higher term, which changes only the metadata and so costs exactly one write, flush, and atomic publish of the metadata file.</summary>
    /// <returns>A task that completes when the publishes finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the log was not initialized.</exception>
    [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerInvoke)]
    public async Task PublishMetadataAsync()
    {
        var log = ThrowHelper.Required(_log, "Benchmark log was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await log.ObserveTermAsync(++_term, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Writes one block to an already open file and flushes it, with the file options of the group log.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the file was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void RawWriteAndFlush()
    {
        var handle = ThrowHelper.Required(_handle, "Benchmark file was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            RandomAccess.Write(handle, _block, 0);
            RandomAccess.FlushToDisk(handle);
        }
    }

    /// <summary>Creates the follower log and the raw file.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dir = new TempDirectory("squirix-group-meta-bench");
        _term = 1;
        Array.Fill(_block, Convert.ToByte('x'));

        _log = new FollowerLog(_dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await _log.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        var options = OperatingSystem.IsWindows() ? FileOptions.WriteThrough : FileOptions.None;
        _handle = File.OpenHandle(Path.Join(_dir, "raw.bin"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, options);
        RandomAccess.SetLength(_handle, BlockBytes);
    }
}
