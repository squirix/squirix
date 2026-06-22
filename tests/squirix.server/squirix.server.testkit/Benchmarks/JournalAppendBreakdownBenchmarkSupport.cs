using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Observability;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Benchmark helpers for journal append path breakdown (encode / enqueue / fsync isolation).</summary>
[SuppressMessage("Design", "MA0109:Add an overload with Span/ReadOnlySpan", Justification = "Benchmark helper accepts materialized entry payloads from setup.")]
public static class JournalAppendBreakdownBenchmarkSupport
{
    private static bool IsQuickMode => string.Equals(System.Environment.GetEnvironmentVariable("SQUIRIX_BENCH_QUICK"), "1", StringComparison.Ordinal);

    /// <summary>Encodes one pipelined binary journal frame into a rented buffer and returns the frame length.</summary>
    /// <param name="cacheNamespace">Cache namespace for the encoded key.</param>
    /// <param name="key">Cache key for the encoded entry.</param>
    /// <param name="payload">Binary cache-entry payload.</param>
    /// <param name="rentedBuffer">Array-pool buffer containing the encoded frame.</param>
    /// <returns>The encoded frame length in bytes.</returns>
    public static int EncodePipelinedPutFrame(string cacheNamespace, string key, byte[] payload, out byte[] rentedBuffer)
    {
        ArgumentNullException.ThrowIfNull(payload);

        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = new CacheKey(cacheNamespace, key),
            PutEntryBytes = payload,
            PutOperationId = string.Empty,
        };
        var bodyLen = BinaryJournalCodec.ComputeFrameBodyLength(record);
        var frameLen = JournalFraming.FrameTotalLength(bodyLen);
        rentedBuffer = ArrayPool<byte>.Shared.Rent(frameLen);
        var body = rentedBuffer.AsSpan(JournalFraming.FrameHeaderSize, bodyLen);
        _ = BinaryJournalCodec.Encode(record, body);
        JournalFraming.WriteFrame(rentedBuffer.AsSpan(0, frameLen), body);
        return frameLen;
    }

    /// <summary>Returns group-commit operations per writer for quick local runs.</summary>
    /// <param name="defaultOperationsPerWriter">Default operations per writer when quick mode is disabled.</param>
    /// <returns>The resolved operations per writer.</returns>
    public static int ResolveGroupCommitOperationsPerWriter(int defaultOperationsPerWriter) =>
        IsQuickMode ? Math.Max(defaultOperationsPerWriter / 10, 100) : defaultOperationsPerWriter;

    /// <summary>Returns group-commit parallel writer count for quick local runs.</summary>
    /// <param name="defaultParallelWriters">Default parallel writer count when quick mode is disabled.</param>
    /// <returns>The resolved parallel writer count.</returns>
    public static int ResolveGroupCommitParallelWriters(int defaultParallelWriters) => IsQuickMode ? Math.Max(defaultParallelWriters / 2, 2) : defaultParallelWriters;

    /// <summary>Returns the benchmark iteration count for quick local runs when <c>SQUIRIX_BENCH_QUICK=1</c>.</summary>
    /// <param name="defaultCount">Default iteration count when quick mode is disabled.</param>
    /// <returns>The resolved iteration count.</returns>
    public static int ResolveOperationsPerInvoke(int defaultCount) => IsQuickMode ? 1_000 : defaultCount;
}
