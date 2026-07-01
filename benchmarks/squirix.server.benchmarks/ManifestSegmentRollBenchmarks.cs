using System;
using System.Buffers;
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
    private const int OverflowPayloadSize = 16_000;
    private JournalBenchmarkHost? _host;
    private CacheKey _overflowKey = new("bench", "overflow");
    private byte[]? _overflowRented;
    private int _rollsPerInvoke;

    /// <summary>Disposes benchmark resources.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_overflowRented is not null)
        {
            ArrayPool<byte>.Shared.Return(_overflowRented);
            _overflowRented = null;
        }

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
        _overflowRented = ArrayPool<byte>.Shared.Rent(OverflowPayloadSize);
        Array.Fill(_overflowRented, Convert.ToByte('z'), 0, OverflowPayloadSize);
        _overflowKey = new CacheKey("bench", "overflow");
    }

    /// <summary>Forces segment rolls via oversized append frames (production roll + manifest path).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the journal coordinator is not pipelined.</exception>
    [Benchmark]
    public async Task RollSegmentViaOverflowAppendAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        if (host.Coordinator is not JournalCoordinator coordinator)
            throw new InvalidOperationException("Benchmark requires a pipelined journal coordinator.");
        var overflowRented = _overflowRented ?? throw new InvalidOperationException("Benchmark overflow payload was not initialized.");
        var overflowPayload = overflowRented.AsMemory(0, OverflowPayloadSize);
        var overflowFrameLen = FrameLength(overflowPayload, _overflowKey);
        for (var i = 0; i < _rollsPerInvoke; i++)
        {
            await FillActiveSegmentNearCapacityAsync(coordinator, overflowFrameLen, CancellationToken.None).ConfigureAwait(false);
            await coordinator.AppendPutAsync(_overflowKey, overflowPayload, null, CancellationToken.None).ConfigureAwait(false);
            await coordinator.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static async Task FillActiveSegmentNearCapacityAsync(JournalCoordinator pipelined, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillRented = ArrayPool<byte>.Shared.Rent(FillPayloadBytes);
        try
        {
            Array.Fill(fillRented, Convert.ToByte('x'), 0, FillPayloadBytes);
            var fillPayload = fillRented.AsMemory(0, FillPayloadBytes);
            var fillKey = new CacheKey("bench", "fill");
            var fillFrameLen = FrameLength(fillPayload, fillKey);
            const long maxBytes = 1024L * 1024L;

            while (pipelined.ActiveSegmentWrittenBytes + fillFrameLen + overflowFrameLen <= maxBytes)
                await pipelined.AppendPutAsync(fillKey, fillPayload, null, cancellationToken).ConfigureAwait(false);

            await pipelined.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(fillRented);
        }
    }

    private static int FrameLength(ReadOnlyMemory<byte> payload, CacheKey key)
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
