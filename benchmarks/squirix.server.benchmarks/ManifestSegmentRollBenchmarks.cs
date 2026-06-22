using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>End-to-end segment roll benchmarks including manifest publish on the journal I/O thread.</summary>
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class ManifestSegmentRollBenchmarks
{
    private const int FillPayloadBytes = 8_192;
    private JournalBenchmarkHost? _host;
    private byte[] _overflowPayload = [];
    private CacheKey _overflowKey = new("bench", "overflow");
    private int _rollsPerInvoke;

    /// <summary>Forces segment rolls via oversized append frames (production roll + manifest path).</summary>
    [Benchmark]
    public async Task RollSegmentViaOverflowAppendAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        if (host.Coordinator is not JournalCoordinator coordinator)
            throw new InvalidOperationException("Benchmark requires a pipelined journal coordinator.");
        var overflowFrameLen = FrameLength(_overflowPayload, _overflowKey);
        for (var i = 0; i < _rollsPerInvoke; i++)
        {
            await FillActiveSegmentNearCapacityAsync(coordinator, overflowFrameLen, CancellationToken.None).ConfigureAwait(false);
            await coordinator.AppendPutAsync(_overflowKey, _overflowPayload, null, CancellationToken.None).ConfigureAwait(false);
            await coordinator.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Disposes benchmark resources.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Creates journal host configured for 1 MiB segments.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _rollsPerInvoke = ManifestBenchmarkSupport.ResolveRollsPerInvoke();
        var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
        var options = new PersistenceOptions
        {
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalMaxSegmentMb = 1,
            JournalMaxSegmentCount = 1024,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = retention,
            SnapshotRetentionCount = retention,
        };
        _host = await JournalBenchmarkHost.CreateAsync("manifest-roll-bench", options, CancellationToken.None).ConfigureAwait(false);
        _overflowPayload = new byte[16_000];
        Array.Fill(_overflowPayload, Convert.ToByte('z'));
        _overflowKey = new CacheKey("bench", "overflow");
    }

    private static async Task FillActiveSegmentNearCapacityAsync(JournalCoordinator pipelined, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = new byte[FillPayloadBytes];
        Array.Fill(fillPayload, Convert.ToByte('x'));
        var fillKey = new CacheKey("bench", "fill");
        var fillFrameLen = FrameLength(fillPayload, fillKey);
        const long maxBytes = 1024L * 1024L;

        while (pipelined.ActiveSegmentWrittenBytes + fillFrameLen + overflowFrameLen <= maxBytes)
            await pipelined.AppendPutAsync(fillKey, fillPayload, null, cancellationToken).ConfigureAwait(false);

        await pipelined.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static int FrameLength(byte[] payload, CacheKey key)
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = payload,
        };
        return JournalFraming.FrameTotalLength(BinaryJournalCodec.ComputeFrameBodyLength(record));
    }
}
