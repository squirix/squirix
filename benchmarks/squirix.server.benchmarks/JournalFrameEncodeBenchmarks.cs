using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of preparing and encoding one binary journal frame into a reused buffer.</summary>
[MemoryDiagnoser]
public class JournalFrameEncodeBenchmarks
{
    private const int BufferSize = 1_024;
    private const int OperationsPerInvoke = 10_000;
    private const int PutEntrySize = 64;
    private byte[] _buffer = [];
    private JournalRecord _put = new();
    private JournalRecord _remove = new();

    /// <summary>Prepares and encodes a put carrying an operation id and a fingerprint.</summary>
    /// <returns>The sum of the encoded frame lengths.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public int EncodePutWithOperationId() => EncodeRepeatedly(_put);

    /// <summary>Prepares and encodes a remove carrying an operation id and a fingerprint.</summary>
    /// <returns>The sum of the encoded frame lengths.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public int EncodeRemoveWithOperationId() => EncodeRepeatedly(_remove);

    /// <summary>Builds the records and the reused destination buffer.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _buffer = new byte[BufferSize];
        var key = new CacheKey("bench", "frame-key");
        _put = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = key,
            MutationOperationId = "0123456789abcdef0123456789abcdef",
            MutationFingerprint = "try-add-entry-async|default|k|abc123",
            PutEntryBytes = new byte[PutEntrySize],
        };
        _remove = new JournalRecord
        {
            Sequence = 2,
            UnixMs = 1,
            Operation = JournalOperationKind.Remove,
            Key = key,
            MutationOperationId = "0123456789abcdef0123456789abcdef",
            MutationFingerprint = "try-add-entry-async|default|k|abc123",
        };
    }

    private int EncodeRepeatedly(JournalRecord record)
    {
        var total = 0;
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var context = BinaryJournalCodec.PrepareEncode(record);
            total += BinaryJournalCodec.Encode(record, _buffer, in context);
        }

        return total;
    }
}
