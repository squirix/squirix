using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>The UTF-8 lengths of one journal record, measured once and reused by the encoder.</summary>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct EncodeContext
{
    private const int FieldLengthPrefixSize = sizeof(ushort);
    private const string IdempotencyOutcomeTooLongMessage = "Idempotency outcome operation id or fingerprint exceeds maximum encoded length.";
    private const string IdempotencyStartedTooLongMessage = "Idempotency started operation id or fingerprint exceeds maximum encoded length.";
    private const string MutationTooLongMessage = "Mutation operation id or fingerprint exceeds maximum encoded length.";
    private const int ResponseLengthPrefixSize = sizeof(int);

    private EncodeContext(int keyNamespaceLength, int keyLength, int payloadUtf8Length, int operationIdLength, int fingerprintLength)
    {
        KeyNamespaceLength = keyNamespaceLength;
        KeyLength = keyLength;
        PayloadUtf8Length = payloadUtf8Length;
        OperationIdLength = operationIdLength;
        FingerprintLength = fingerprintLength;
    }

    internal int BodyLength => BinaryJournalCodec.FixedPrefixSize + KeyNamespaceLength + KeyLength + PayloadUtf8Length;

    /// <summary>Gets the UTF-8 length of the fingerprint, zero when the record carries none.</summary>
    internal int FingerprintLength { get; }

    internal int KeyLength { get; }

    internal int KeyNamespaceLength { get; }

    /// <summary>Gets the UTF-8 length of the operation id, zero when the record carries none.</summary>
    internal int OperationIdLength { get; }

    internal int PayloadUtf8Length { get; }

    internal static EncodeContext From(JournalRecord record)
    {
        var namespaceLength = Encoding.UTF8.GetByteCount(record.Key.Namespace);
        var keyLength = Encoding.UTF8.GetByteCount(record.Key.Key);
        return record.Operation switch
        {
            JournalOperationKind.Put => ForMutation(record, namespaceLength, keyLength, record.PutEntryBytes.Length),
            JournalOperationKind.Remove => ForMutation(record, namespaceLength, keyLength, 0),
            JournalOperationKind.IdempotencyOutcome => ForIdempotency(
                record,
                namespaceLength,
                keyLength,
                ResponseLengthPrefixSize + record.IdempotencyResponseBytes.Length,
                IdempotencyOutcomeTooLongMessage),
            JournalOperationKind.IdempotencyStarted => ForIdempotency(record, namespaceLength, keyLength, 0, IdempotencyStartedTooLongMessage),
            JournalOperationKind.AwaitDurabilityCommit or JournalOperationKind.WaitForStartup or JournalOperationKind.SnapshotCut
                or JournalOperationKind.UnderSnapshotBarrier => throw new NotSupportedException("The length of the journal operation cannot be determined."),
            _ => throw new NotSupportedException("The length of the journal operation cannot be determined."),
        };
    }

    private static EncodeContext ForIdempotency(JournalRecord record, int namespaceLength, int keyLength, int trailingLength, string tooLongMessage)
    {
        var operationIdLength = Encoding.UTF8.GetByteCount(record.IdempotencyOperationId ?? string.Empty);
        var fingerprintLength = Encoding.UTF8.GetByteCount(record.IdempotencyFingerprint ?? string.Empty);
        if (operationIdLength > ushort.MaxValue || fingerprintLength > ushort.MaxValue)
            throw new InvalidDataException(tooLongMessage);

        var payloadLength = FieldLengthPrefixSize + operationIdLength + FieldLengthPrefixSize + fingerprintLength + trailingLength;
        return new EncodeContext(namespaceLength, keyLength, payloadLength, operationIdLength, fingerprintLength);
    }

    private static EncodeContext ForMutation(JournalRecord record, int namespaceLength, int keyLength, int entryLength)
    {
        var operationId = record.MutationOperationId;
        if (operationId == null)
            return new EncodeContext(namespaceLength, keyLength, entryLength, 0, 0);

        var operationIdLength = Encoding.UTF8.GetByteCount(operationId);
        var fingerprint = record.MutationFingerprint;
        var fingerprintLength = fingerprint == null ? 0 : Encoding.UTF8.GetByteCount(fingerprint);
        if (operationIdLength > ushort.MaxValue || fingerprintLength > ushort.MaxValue)
            throw new InvalidDataException(MutationTooLongMessage);

        var payloadLength = FieldLengthPrefixSize + operationIdLength + FieldLengthPrefixSize + fingerprintLength + entryLength;
        return new EncodeContext(namespaceLength, keyLength, payloadLength, operationIdLength, fingerprintLength);
    }
}
