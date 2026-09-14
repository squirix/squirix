using System;
using System.Text;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling.Codec;

[Immutable]
internal sealed record EncodeContext
{
    private EncodeContext(Utf8KeyLengths keyUtf8, int payloadUtf8Length)
    {
        KeyUtf8 = keyUtf8;
        PayloadUtf8Length = payloadUtf8Length;
    }

    internal int BodyLength => BinaryJournalCodec.FixedPrefixSize + KeyUtf8.TotalLength + PayloadUtf8Length;

    internal int KeyLength => KeyUtf8.KeyLength;

    internal int KeyNamespaceLength => KeyUtf8.NamespaceLength;

    internal int PayloadUtf8Length { get; }

    private Utf8KeyLengths KeyUtf8 { get; }

    internal static EncodeContext From(JournalRecord record)
    {
        var keyUtf8 = Utf8KeyLengths.FromKey(record.Key);
        var payloadUtf8Length = GetOperationPayloadLength(record);
        return new EncodeContext(keyUtf8, payloadUtf8Length);
    }

    private static int GetOperationPayloadLength(JournalRecord record)
    {
        var mutationOperationIdPrefix = MutationOperationIdCodec.EncodeMutationOperationIdPrefixLength(record.MutationOperationId);
        return record.Operation switch
        {
            JournalOperationKind.Put => mutationOperationIdPrefix + record.PutEntryBytes.Length,
            JournalOperationKind.TouchExpiration => mutationOperationIdPrefix + 8,
            JournalOperationKind.Remove or JournalOperationKind.RemoveExpiration => mutationOperationIdPrefix,
            JournalOperationKind.IdempotencyOutcome => 2 + Encoding.UTF8.GetByteCount(record.IdempotencyOperationId ?? string.Empty) + 2 +
                                                       Encoding.UTF8.GetByteCount(record.IdempotencyFingerprint ?? string.Empty) + 4 + record.IdempotencyResponseBytes.Length,
            JournalOperationKind.IdempotencyStarted => 2 + Encoding.UTF8.GetByteCount(record.IdempotencyOperationId ?? string.Empty) + 2 +
                                                        Encoding.UTF8.GetByteCount(record.IdempotencyFingerprint ?? string.Empty),
            JournalOperationKind.AwaitDurabilityCommit or JournalOperationKind.WaitForStartup or JournalOperationKind.MaintenanceExclusive
                or JournalOperationKind.SnapshotCut or JournalOperationKind.UnderSnapshotBarrier => throw new NotSupportedException("The length of the journal operation cannot be determined."),
            _ => throw new NotSupportedException("The length of the journal operation cannot be determined."),
        };
    }

    [Immutable]
    private sealed record Utf8KeyLengths
    {
        private Utf8KeyLengths(int namespaceLength, int keyLength)
        {
            NamespaceLength = namespaceLength;
            KeyLength = keyLength;
        }

        internal int KeyLength { get; }

        internal int NamespaceLength { get; }

        internal int TotalLength => NamespaceLength + KeyLength;

        internal static Utf8KeyLengths FromKey(CacheKey key) => new(Encoding.UTF8.GetByteCount(key.Namespace), Encoding.UTF8.GetByteCount(key.Key));
    }
}
