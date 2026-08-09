using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable on-disk management of one replica group's installable snapshot.</summary>
/// <remarks>
///     <para>
///     The published snapshot file is validated (header, declared length, CRC32C, bounded size) before it is trusted,
///     and publication uses a temp file with an atomic replacement.
///     </para>
///     <para>
///     This type is a storage-only component: it writes and validates snapshot files but does not interpret the cache
///     state. Coordinating group membership and installing snapshot payloads into memory belongs to an outer layer.
///     </para>
/// </remarks>
[Immutable]
internal sealed class GroupSnapshotStore : IFollowerLogSnapshotStore
{
    internal const int DefaultMaxSnapshotBytes = 64 * 1024 * 1024;
    private readonly string _groupId;

    private readonly int _maxSnapshotBytes;
    private readonly string _snapshotPath;
    private readonly string _snapshotTempPath;

    internal GroupSnapshotStore(string persistenceRoot, string groupId, int maxSnapshotBytes = DefaultMaxSnapshotBytes)
    {
        if (maxSnapshotBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxSnapshotBytes), "Maximum snapshot size must be positive.");

        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        _groupId = groupId;
        _snapshotPath = GroupStoragePaths.GetSnapshotPath(persistenceRoot, groupId);
        _snapshotTempPath = GroupStoragePaths.GetSnapshotTempPath(persistenceRoot, groupId);
        _maxSnapshotBytes = maxSnapshotBytes;
    }

    /// <inheritdoc />
    bool IFollowerLogSnapshotStore.SnapshotExists => SnapshotExists;

    /// <inheritdoc />
    string IFollowerLogSnapshotStore.SnapshotPath => _snapshotPath;

    /// <summary>Gets a value indicating whether a published snapshot currently exists.</summary>
    internal bool SnapshotExists => File.Exists(_snapshotPath);

    /// <inheritdoc />
    Task IFollowerLogSnapshotStore.PublishAsync(GroupSnapshot snapshot, CancellationToken cancellationToken) => PublishAsync(snapshot, cancellationToken);

    /// <inheritdoc />
    Task<GroupSnapshot?> IFollowerLogSnapshotStore.ReadPublishedAsync(CancellationToken cancellationToken) => ReadPublishedAsync(cancellationToken);

    /// <summary>Writes a snapshot to a temp file, flushes it, and atomically publishes it.</summary>
    /// <param name="snapshot">The snapshot to persist.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the snapshot is durably published.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the snapshot committed outcomes are null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the snapshot violates its boundary invariants or exceeds the configured maximum size.</exception>
    /// <remarks>
    ///     <para>
    ///     Callers must serialize publication per instance: concurrent calls to <see cref="PublishAsync" /> on the same
    ///     <see cref="GroupSnapshotStore" /> are not supported. Publication writes to a fixed per-group temp path opened
    ///     with exclusive access (<c language="csharp">FileMode.Create</c>, <c language="csharp">FileShare.None</c>), so overlapping publications fail with
    ///     an <see cref="IOException" /> or silently overwrite each other's temp file.
    ///     </para>
    ///     <para>
    ///     For example, <c language="csharp">FollowerLog</c> satisfies this contract by invoking publication only while holding its
    ///     single-writer gate (<c language="csharp">CreateSnapshotAsync</c> and <c language="csharp">InstallSnapshotAsync</c>).
    ///     </para>
    /// </remarks>
    internal async Task PublishAsync(GroupSnapshot snapshot, CancellationToken cancellationToken)
    {
        var stableSnapshot = ValidateForPublish(snapshot);

        var payloadLength = GroupSnapshotCodec.ComputeSnapshotEncodedLength(stableSnapshot);
        long fileLength = GroupSnapshotCodec.SnapshotHeaderByteCount;
        fileLength += payloadLength;
        fileLength += GroupSnapshotCodec.Crc32ByteCount;
        if (fileLength > _maxSnapshotBytes)
            throw new InvalidOperationException($"Replica group snapshot exceeds the maximum configured size of {_maxSnapshotBytes} bytes.");

        cancellationToken.ThrowIfCancellationRequested();
        var fileLengthBytes = int.CreateChecked(fileLength);
        var bytes = ArrayPool<byte>.Shared.Rent(fileLengthBytes);
        var published = false;
        try
        {
            const int headerLen = GroupSnapshotCodec.SnapshotHeaderByteCount;
            GroupSnapshotEncoder.WriteSnapshotFileHeader(bytes.AsSpan(0, headerLen), payloadLength);
            GroupSnapshotEncoder.EncodeSnapshot(bytes.AsSpan(headerLen, payloadLength), stableSnapshot);
            var crc = Crc32C.Compute(bytes.AsSpan(headerLen, payloadLength));
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(headerLen + payloadLength), crc);

            const FileOptions options = FileOptions.WriteThrough;
            using (var handle = File.OpenHandle(_snapshotTempPath, FileMode.Create, FileAccess.Write, FileShare.None, options))
            {
                await RandomAccess.WriteAsync(handle, bytes.AsMemory(0, fileLengthBytes), 0, cancellationToken).ConfigureAwait(false);
                if (!OperatingSystem.IsWindows())
                    RandomAccess.FlushToDisk(handle);
            }

            _ = FileEx.PublishFile(_snapshotTempPath, _snapshotPath);
            published = true;
        }
        finally
        {
            ArrayPool<byte>.Shared.ReturnCleared(bytes);
            if (!published)
                _ = FileEx.TryDeleteFile(_snapshotTempPath);
        }
    }

    /// <summary>Reads and validates the published snapshot file, if any.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The validated snapshot, or <see langword="null" /> when no snapshot is published.</returns>
    /// <exception cref="InvalidDataException">Thrown when the published snapshot fails structural or CRC validation.</exception>
    /// <remarks>
    /// The read handle allows delete-sharing so a concurrent publication can replace the file while it is being read;
    /// without it, Windows <c language="csharp">File.Replace</c> fails for the duration of the read.
    /// </remarks>
    internal async Task<GroupSnapshot?> ReadPublishedAsync(CancellationToken cancellationToken)
    {
        // A concurrent publication can replace the file between the existence check and the open; a vanished file
        // means "no published snapshot", matching this method's documented outcome contract. The handle opens
        // with FileShare.Delete, so a delete landing after the open does not interrupt the in-flight read.
        if (!File.Exists(_snapshotPath))
            return null;

        try
        {
            using var handle = File.OpenHandle(_snapshotPath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
            var length = RandomAccess.GetLength(handle);
            if (length > _maxSnapshotBytes)
                throw new InvalidDataException($"Replica group snapshot at '{_snapshotPath}' exceeds the maximum configured size of {_maxSnapshotBytes} bytes.");

            var byteCount = int.CreateTruncating(length);
            var bytes = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                var total = 0;
                while (total < byteCount)
                {
                    var read = await RandomAccess.ReadAsync(handle, bytes.AsMemory(total, byteCount - total), total, cancellationToken).ConfigureAwait(false);
                    if (read == 0)
                        break;

                    total += read;
                }

                byte observedVersion = 0;
                if (total != byteCount || !GroupSnapshotCodec.TryValidateAndDecode(bytes.AsSpan(0, byteCount), _maxSnapshotBytes, out var snapshot, out observedVersion))
                {
                    if (observedVersion != 0 && observedVersion != GroupSnapshotCodec.SnapshotVersion)
                    {
                        throw new InvalidDataException(
                            $"Replica group snapshot at '{_snapshotPath}' uses format version {observedVersion}; version {GroupSnapshotCodec.SnapshotVersion} is required.");
                    }

                    throw new InvalidDataException($"Replica group snapshot at '{_snapshotPath}' is corrupt.");
                }

                if (!string.Equals(snapshot.GroupId, _groupId, StringComparison.Ordinal))
                    throw new InvalidDataException($"Replica group snapshot at '{_snapshotPath}' belongs to a different group.");

                return snapshot;
            }
            finally
            {
                ArrayPool<byte>.Shared.ReturnCleared(bytes);
            }
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return null;
        }
    }

    /// <summary>Materializes the committed outcomes into a private copy validated against the snapshot boundary.</summary>
    /// <param name="snapshot">The snapshot whose committed outcomes are materialized.</param>
    /// <returns>The validated per-record copy safe to encode twice.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the snapshot committed outcomes are null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when an outcome is unresolved or its log index exceeds the boundary.</exception>
    private static GroupIdempotencyRecord[] MaterializeValidatedOutcomes(GroupSnapshot snapshot)
    {
        var outcomes = snapshot.CommittedOutcomes ?? throw new ArgumentNullException(nameof(snapshot), "Snapshot committed outcomes must not be null.");

        var records = new GroupIdempotencyRecord[outcomes.Count];
        for (var i = 0; i < records.Length; i++)
        {
            var record = outcomes[i];
            if (record.ResolvedUtc == null)
                throw new InvalidOperationException($"Replica group snapshot outcome at log index {record.LogIndex} is unresolved.");

            if (record.LogIndex > snapshot.LastIncludedIndex)
                throw new InvalidOperationException($"Replica group snapshot outcome log index {record.LogIndex} exceeds the last included index {snapshot.LastIncludedIndex}.");

            records[i] = record;
        }

        return records;
    }

    /// <summary>Validates the snapshot boundary invariants and materializes a stable outcome list for encoding.</summary>
    /// <param name="snapshot">The snapshot to validate.</param>
    /// <returns>A snapshot whose committed outcomes are a private copy safe to encode twice.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the snapshot committed outcomes are null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the snapshot violates its boundary invariants.</exception>
    private GroupSnapshot ValidateForPublish(GroupSnapshot snapshot)
    {
        ValidatePublishBoundaryInvariants(snapshot);

        // CommittedOutcomes is an IReadOnlyList a caller may keep mutating. Materialize it once so the sizing
        // pass and the encoding pass observe identical elements; otherwise a list that changes between the two
        // passes either writes past the sized span or publishes a payload length larger than the encoded bytes.
        return snapshot with { CommittedOutcomes = MaterializeValidatedOutcomes(snapshot) };
    }

    /// <summary>Rejects at write time every invariant the on-disk decoder and recovery refuse downstream.</summary>
    /// <param name="snapshot">The snapshot about to be published.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the commit index is below the included index, the included term is zero for a non-empty snapshot,
    /// or the snapshot belongs to another group.
    /// </exception>
    private void ValidatePublishBoundaryInvariants(GroupSnapshot snapshot)
    {
        // The on-disk decoder rejects a payload whose commit index falls below its included index or whose
        // outcome log index exceeds the boundary. Reject the same invariants here, at write time, so a file that
        // this store will later classify as corrupt is never published in the first place.
        if (snapshot.CommitIndex < snapshot.LastIncludedIndex)
            throw new InvalidOperationException($"Replica group snapshot commit index {snapshot.CommitIndex} is below the last included index {snapshot.LastIncludedIndex}.");

        // Recovery refuses a non-empty snapshot whose included term is zero: term zero cannot be distinguished
        // from an unset sentinel and would render the group unrecoverable. Mirror that refusal at write time.
        if (snapshot.LastIncludedIndex != 0 && snapshot.LastIncludedTerm == 0)
            throw new InvalidOperationException($"Replica group snapshot at included index {snapshot.LastIncludedIndex} has a zero included term.");

        // ReadPublishedAsync refuses a snapshot whose group id differs from this store's group. Reject the
        // mismatch before the atomic replacement destroys the previously published, readable snapshot.
        if (!string.Equals(snapshot.GroupId, _groupId, StringComparison.Ordinal))
            throw new InvalidOperationException($"Replica group snapshot belongs to group '{snapshot.GroupId}' but this store owns group '{_groupId}'.");
    }

    /// <summary>Wire-value mapping for <see cref="GroupRecordKind" /> in snapshot payloads; the single source of truth for both directions.</summary>
    private static class GroupRecordKindCodec
    {
        /// <summary>Encodes a record kind into its wire value, throwing on unsupported kinds.</summary>
        /// <param name="kind">The record kind to encode.</param>
        /// <returns>The stable wire value of the kind.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="kind" /> is not a named record kind.</exception>
        internal static int ToWire(GroupRecordKind kind) => kind switch
        {
            GroupRecordKind.UserMutation => 1,
            GroupRecordKind.Expiration => 2,
            GroupRecordKind.Metadata => 3,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported record kind."),
        };

        /// <summary>Decodes a wire value into a record kind.</summary>
        /// <param name="value">The wire value read from a snapshot payload.</param>
        /// <param name="kind">The decoded record kind when the value is known.</param>
        /// <returns><see langword="true" /> when the wire value is known; otherwise <see langword="false" />.</returns>
        internal static bool TryFromWire(int value, out GroupRecordKind kind)
        {
            switch (value)
            {
                case 1:
                    kind = GroupRecordKind.UserMutation;
                    return true;
                case 2:
                    kind = GroupRecordKind.Expiration;
                    return true;
                case 3:
                    kind = GroupRecordKind.Metadata;
                    return true;
                default:
                    kind = default;
                    return false;
            }
        }
    }

    /// <summary>Binary framing constants and length/validation orchestration for replica-group snapshot files.</summary>
    private static class GroupSnapshotCodec
    {
        /// <summary>CRC32C checksum byte count appended after the snapshot payload.</summary>
        internal const int Crc32ByteCount = 4;

        /// <summary>Length-prefix byte count for the topology-fingerprint byte field.</summary>
        internal const int FingerprintLengthPrefixByteCount = 4;

        /// <summary>Length-prefix byte count for the group-id string field.</summary>
        internal const int GroupIdLengthPrefixByteCount = 4;

        /// <summary>
        /// Minimum encoded byte count of a single committed idempotency outcome:
        /// scope length(2) + operation-id length(2) + fingerprint length(4) + outcome-payload length(4)
        /// + kind(4) + created-utc(8) + resolved-present(1) + resolved-utc(8) + log-index(8) + term(8).
        /// </summary>
        internal const int MinOutcomeEncodedByteCount = OutcomeScopeLengthPrefixByteCount + OutcomeOperationIdLengthPrefixByteCount + OutcomeFingerprintLengthPrefixByteCount +
                                                        OutcomePayloadLengthPrefixByteCount + OutcomeKindByteCount + OutcomeCreatedUtcByteCount + OutcomeResolvedPresentByteCount +
                                                        OutcomeResolvedUtcByteCount + OutcomeLogIndexByteCount + OutcomeTermByteCount;

        /// <summary>Length-prefix byte count for the committed-outcome count field.</summary>
        internal const int OutcomeCountLengthPrefixByteCount = 4;

        /// <summary>Byte count of the outcome created-UTC field.</summary>
        internal const int OutcomeCreatedUtcByteCount = 8;

        /// <summary>Length-prefix byte count for the outcome fingerprint byte field.</summary>
        internal const int OutcomeFingerprintLengthPrefixByteCount = 4;

        /// <summary>Byte count of the outcome record kind field.</summary>
        internal const int OutcomeKindByteCount = 4;

        /// <summary>Byte count of the outcome log-index field.</summary>
        internal const int OutcomeLogIndexByteCount = 8;

        /// <summary>Length-prefix byte count for the outcome payload byte field.</summary>
        internal const int OutcomePayloadLengthPrefixByteCount = 4;

        /// <summary>Byte count of the outcome resolved-present flag.</summary>
        internal const int OutcomeResolvedPresentByteCount = 1;

        /// <summary>Byte count of the outcome resolved-UTC field.</summary>
        internal const int OutcomeResolvedUtcByteCount = 8;

        /// <summary>Byte count of the outcome term field.</summary>
        internal const int OutcomeTermByteCount = 8;

        /// <summary>Fixed payload fields: generation(8) + lastIncludedTerm(8) + lastIncludedIndex(8) + commitIndex(8).</summary>
        internal const int SnapshotFixedByteCount = SnapshotFixedFieldByteCount * 4;

        /// <summary>Byte count of a fixed 64-bit snapshot payload field.</summary>
        internal const int SnapshotFixedFieldByteCount = 8;

        /// <summary>Fixed snapshot header byte count: magic(4) + version(1) + payloadLength(4).</summary>
        internal const int SnapshotHeaderByteCount = SnapshotMagicByteCount + SnapshotVersionByteCount + SnapshotPayloadLengthByteCount;

        /// <summary>Snapshot file magic bytes, <c language="csharp">"SQRS"</c>.</summary>
        internal const uint SnapshotMagic = 0x53525153u;

        /// <summary>Byte count of the snapshot file magic.</summary>
        internal const int SnapshotMagicByteCount = 4;

        /// <summary>Byte offset of the payload-length field within the snapshot header (after magic and version).</summary>
        internal const int SnapshotPayloadLengthOffset = SnapshotMagicByteCount + SnapshotVersionByteCount;

        /// <summary>Total length-prefix bytes that precede the first committed outcome in the snapshot payload.</summary>
        internal const int SnapshotPayloadPrefixByteCount = GroupIdLengthPrefixByteCount + FingerprintLengthPrefixByteCount + OutcomeCountLengthPrefixByteCount;

        /// <summary>Snapshot format version.</summary>
        internal const byte SnapshotVersion = 2;

        /// <summary>Length-prefix byte count for a 16-bit-prefixed UTF-8 string field.</summary>
        internal const int String16LengthPrefixByteCount = 2;

        /// <summary>Length-prefix byte count for the outcome operation-id string field.</summary>
        private const int OutcomeOperationIdLengthPrefixByteCount = 2;

        /// <summary>Length-prefix byte count for the outcome scope string field.</summary>
        private const int OutcomeScopeLengthPrefixByteCount = 2;

        /// <summary>Byte count of the payload-length field in the snapshot header.</summary>
        private const int SnapshotPayloadLengthByteCount = 4;

        /// <summary>Byte count of the snapshot format version field.</summary>
        private const int SnapshotVersionByteCount = 1;

        /// <summary>Computes the exact encoded payload length of a snapshot.</summary>
        /// <param name="snapshot">The snapshot to encode.</param>
        /// <returns>The snapshot payload length in bytes.</returns>
        /// <exception cref="ArgumentNullException">Thrown when the snapshot group identifier or committed outcomes are null.</exception>
        /// <exception cref="InvalidDataException">Thrown when a field exceeds its maximum encoded length.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the total encoded length exceeds <c language="csharp">int.MaxValue</c>.</exception>
        internal static int ComputeSnapshotEncodedLength(GroupSnapshot snapshot)
        {
            ArgumentNullException.ThrowIfNull(snapshot.GroupId);
            ArgumentNullException.ThrowIfNull(snapshot.CommittedOutcomes);

            long length = SnapshotFixedByteCount;
            length += GroupIdLengthPrefixByteCount + Encoding.UTF8.GetByteCount(snapshot.GroupId);
            length += FingerprintLengthPrefixByteCount + snapshot.TopologyFingerprint.Length;
            length += OutcomeCountLengthPrefixByteCount;

            var outcomes = snapshot.CommittedOutcomes;
            for (var i = 0; i < outcomes.Count; i++)
            {
                var outcome = outcomes[i];
                length += ComputeOutcomeEncodedLength(in outcome);
            }

            if (length > int.MaxValue)
                throw new InvalidOperationException($"Replica group snapshot encoded length {length} exceeds the maximum.");

            return int.CreateTruncating(length);
        }

        /// <summary>Validates a complete snapshot file: header, declared length, checksum, and bounded size.</summary>
        /// <param name="fileBytes">The entire snapshot file bytes.</param>
        /// <param name="maxSnapshotBytes">The maximum accepted snapshot file size in bytes.</param>
        /// <param name="snapshot">The decoded snapshot when the file is valid.</param>
        /// <param name="observedVersion">The format version byte read from the header, or <c language="csharp">0</c> when the header was not reached.</param>
        /// <returns><see langword="true" /> when the file is structurally valid, sized, and CRC-valid.</returns>
        internal static bool TryValidateAndDecode(ReadOnlySpan<byte> fileBytes, int maxSnapshotBytes, out GroupSnapshot snapshot, out byte observedVersion)
        {
            snapshot = default;
            observedVersion = 0;
            if (fileBytes.Length < SnapshotHeaderByteCount + Crc32ByteCount)
                return false;

            if (fileBytes.Length > maxSnapshotBytes)
                return false;

            if (BinaryPrimitives.ReadUInt32LittleEndian(fileBytes[..SnapshotMagicByteCount]) != SnapshotMagic)
                return false;

            observedVersion = fileBytes[SnapshotMagicByteCount];
            if (observedVersion != SnapshotVersion)
                return false;

            var declaredLength = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.Slice(SnapshotPayloadLengthOffset, SnapshotPayloadLengthByteCount));
            if (declaredLength < 0)
                return false;

            // header(9) + payload + crc(4)
            if (declaredLength != fileBytes.Length - SnapshotHeaderByteCount - Crc32ByteCount)
                return false;

            var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(fileBytes[^Crc32ByteCount..]);
            var payload = fileBytes.Slice(SnapshotHeaderByteCount, declaredLength);
            if (Crc32C.Compute(payload) != storedCrc)
                return false;

            return GroupSnapshotDecoder.TryDecodeSnapshot(payload, out snapshot);
        }

        /// <summary>Computes the exact encoded length of one committed idempotency outcome.</summary>
        /// <param name="record">The outcome to encode.</param>
        /// <returns>The outcome payload length in bytes.</returns>
        /// <exception cref="InvalidDataException">Thrown when a field exceeds its maximum encoded length.</exception>
        private static long ComputeOutcomeEncodedLength(in GroupIdempotencyRecord record)
        {
            if (record.ResolvedUtc == null)
                throw new InvalidDataException("Snapshot outcome must be resolved.");
            var scopeBytes = Encoding.UTF8.GetByteCount(record.OperationScope);
            var operationIdBytes = Encoding.UTF8.GetByteCount(record.OperationId);
            if (scopeBytes > ushort.MaxValue || operationIdBytes > ushort.MaxValue)
                throw new InvalidDataException("Replica snapshot operation identity exceeds maximum encoded length.");

            long encodedLength = MinOutcomeEncodedByteCount;
            encodedLength += scopeBytes;
            encodedLength += operationIdBytes;
            encodedLength += record.OperationFingerprint.Length;
            encodedLength += record.OutcomePayload.Length;
            return encodedLength;
        }
    }

    /// <summary>Higher-level snapshot payload decoding orchestration.</summary>
    private static class GroupSnapshotDecoder
    {
        /// <summary>Decodes the payload fields and committed idempotency outcomes.</summary>
        /// <param name="buffer">The snapshot payload bytes without the trailing checksum.</param>
        /// <param name="snapshot">The decoded snapshot when the payload is valid.</param>
        /// <returns><see langword="true" /> when the payload is structurally valid; otherwise <see langword="false" />.</returns>
        internal static bool TryDecodeSnapshot(ReadOnlySpan<byte> buffer, out GroupSnapshot snapshot)
        {
            snapshot = default;
            if (!GroupSnapshotPrimitiveReader.TryReadSnapshotHeader(buffer, out var header))
                return false;

            // A snapshot covers up to LastIncludedIndex, which is committed state, so its carried CommitIndex must be at
            // least LastIncludedIndex. A snapshot whose CommitIndex falls below LastIncludedIndex would let installation
            // persist an applied watermark (LastAppliedIndex) beyond the commit watermark, suppressing entries as applied.
            if (header.CommitIndex < header.LastIncludedIndex)
                return false;

            var offset = header.Offset;
            if (!GroupSnapshotPrimitiveReader.TryReadString(buffer, ref offset, out var groupId) || !GroupSnapshotPrimitiveReader.TryReadBytes(
                    buffer,
                    GroupSnapshotCodec.FingerprintLengthPrefixByteCount,
                    ref offset,
                    out var fingerprint))
                return false;
            if (buffer.Length - offset < GroupSnapshotCodec.OutcomeCountLengthPrefixByteCount)
                return false;

            var outcomeCount = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.OutcomeCountLengthPrefixByteCount;
            if (!TryReadOutcomes(buffer, ref offset, outcomeCount, header.LastIncludedIndex, out var outcomes))
                return false;

            if (offset != buffer.Length)
                return false;

            snapshot = new GroupSnapshot(groupId, fingerprint, header.Generation, header.LastIncludedTerm, header.LastIncludedIndex, header.CommitIndex, outcomes);
            return true;
        }

        /// <summary>Reads and validates one committed idempotency outcome from a snapshot payload.</summary>
        /// <param name="buffer">The snapshot payload bytes.</param>
        /// <param name="offset">The current read offset, advanced past the outcome.</param>
        /// <param name="lastIncludedIndex">The snapshot's last included index; outcomes above this boundary are rejected.</param>
        /// <param name="record">The decoded idempotency record.</param>
        /// <returns><see langword="true" /> when the outcome is valid; otherwise <see langword="false" />.</returns>
        private static bool TryReadOutcome(ReadOnlySpan<byte> buffer, ref int offset, ulong lastIncludedIndex, out GroupIdempotencyRecord record)
        {
            record = default;
            if (!TryReadOutcomeIdentity(buffer, ref offset, out var identity) || !TryReadOutcomeTimes(buffer, ref offset, out var times))
                return false;

            record = new GroupIdempotencyRecord(
                identity.Scope,
                identity.OperationId,
                identity.Fingerprint,
                identity.Outcome,
                identity.Kind,
                times.Created,
                times.Resolved,
                times.LogIndex,
                times.Term);

            // A well-formed snapshot carries outcomes only for indices it covers. An outcome above the snapshot
            // boundary would restore the state the snapshot does not own, so reject the payload as corrupt.
            return record.ResolvedUtc != null && record.LogIndex <= lastIncludedIndex;
        }

        private static bool TryReadOutcomeIdentity(ReadOnlySpan<byte> buffer, ref int offset, out OutcomeIdentity identity)
        {
            identity = default;
            if (!GroupSnapshotPrimitiveReader.TryReadString16(buffer, ref offset, out var scope) ||
                !GroupSnapshotPrimitiveReader.TryReadString16(buffer, ref offset, out var operationId) ||
                !GroupSnapshotPrimitiveReader.TryReadBytes(buffer, GroupSnapshotCodec.OutcomeFingerprintLengthPrefixByteCount, ref offset, out var fingerprint) ||
                !GroupSnapshotPrimitiveReader.TryReadBytes(buffer, GroupSnapshotCodec.OutcomePayloadLengthPrefixByteCount, ref offset, out var outcome) ||
                buffer.Length - offset < GroupSnapshotCodec.OutcomeKindByteCount)
                return false;

            var kindValue = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.OutcomeKindByteCount;
            if (!GroupRecordKindCodec.TryFromWire(kindValue, out var recordKind))
                return false;

            identity = new OutcomeIdentity(scope, operationId, fingerprint, outcome, recordKind);
            return true;
        }

        private static bool TryReadOutcomeTimes(ReadOnlySpan<byte> buffer, ref int offset, out OutcomeTimes times)
        {
            times = default;
            if (buffer.Length - offset < GroupSnapshotCodec.OutcomeCreatedUtcByteCount + GroupSnapshotCodec.OutcomeResolvedPresentByteCount +
                GroupSnapshotCodec.OutcomeResolvedUtcByteCount + GroupSnapshotCodec.OutcomeLogIndexByteCount + GroupSnapshotCodec.OutcomeTermByteCount)
                return false;

            var createdMs = BinaryPrimitives.ReadInt64LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.OutcomeCreatedUtcByteCount;
            var resolvedPresent = buffer[offset];
            offset += GroupSnapshotCodec.OutcomeResolvedPresentByteCount;
            if (resolvedPresent is not 0 and not 1)
                return false;
            var resolvedMs = BinaryPrimitives.ReadInt64LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.OutcomeResolvedUtcByteCount;
            var logIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.OutcomeLogIndexByteCount;
            var term = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.OutcomeTermByteCount;
            if (!GroupSnapshotPrimitiveReader.TryCreateUtc(createdMs, out var created))
                return false;

            DateTime? resolved = null;
            if (resolvedPresent == 1)
            {
                if (!GroupSnapshotPrimitiveReader.TryCreateUtc(resolvedMs, out var resolvedValue))
                    return false;
                resolved = resolvedValue;
            }

            times = new OutcomeTimes(created, resolved, logIndex, term);
            return true;
        }

        private static bool TryReadOutcomes(ReadOnlySpan<byte> buffer, ref int offset, int outcomeCount, ulong lastIncludedIndex, out List<GroupIdempotencyRecord> outcomes)
        {
            outcomes = [];
            if (outcomeCount < 0)
                return false;

            if (outcomeCount > (buffer.Length - offset) / GroupSnapshotCodec.MinOutcomeEncodedByteCount)
                return false;

            outcomes = new List<GroupIdempotencyRecord>(outcomeCount);
            for (var i = 0; i < outcomeCount; i++)
            {
                if (!TryReadOutcome(buffer, ref offset, lastIncludedIndex, out var record))
                    return false;
                outcomes.Add(record);
            }

            return true;
        }

        [Immutable]
        private readonly record struct OutcomeIdentity(string Scope, string OperationId, ReadOnlyMemory<byte> Fingerprint, ReadOnlyMemory<byte> Outcome, GroupRecordKind Kind);

        [Immutable]
        private readonly record struct OutcomeTimes(DateTime Created, DateTime? Resolved, ulong LogIndex, ulong Term);
    }

    /// <summary>Snapshot payload encoding primitives for replica-group snapshot files.</summary>
    private static class GroupSnapshotEncoder
    {
        /// <summary>Encodes a snapshot into a caller-provided buffer of at least the computed payload length.</summary>
        /// <param name="buffer">The destination payload buffer.</param>
        /// <param name="snapshot">The snapshot to encode.</param>
        /// <exception cref="ArgumentNullException">Thrown when the snapshot group identifier or committed outcomes are null.</exception>
        internal static void EncodeSnapshot(Span<byte> buffer, GroupSnapshot snapshot)
        {
            ArgumentNullException.ThrowIfNull(snapshot.GroupId);
            ArgumentNullException.ThrowIfNull(snapshot.CommittedOutcomes);

            var offset = 0;
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], snapshot.ConfigurationGeneration);
            offset += GroupSnapshotCodec.SnapshotFixedFieldByteCount;
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], snapshot.LastIncludedTerm);
            offset += GroupSnapshotCodec.SnapshotFixedFieldByteCount;
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], snapshot.LastIncludedIndex);
            offset += GroupSnapshotCodec.SnapshotFixedFieldByteCount;
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], snapshot.CommitIndex);
            offset += GroupSnapshotCodec.SnapshotFixedFieldByteCount;

            WriteString(buffer, snapshot.GroupId, ref offset);
            WriteBytes(buffer, snapshot.TopologyFingerprint.Span, GroupSnapshotCodec.FingerprintLengthPrefixByteCount, ref offset);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], snapshot.CommittedOutcomes.Count);
            offset += GroupSnapshotCodec.OutcomeCountLengthPrefixByteCount;

            var outcomes = snapshot.CommittedOutcomes;
            for (var i = 0; i < outcomes.Count; i++)
            {
                var outcome = outcomes[i];
                WriteOutcome(buffer, in outcome, ref offset);
            }
        }

        /// <summary>Writes the snapshot file header: magic, version, and payload length.</summary>
        /// <param name="destination">The destination buffer.</param>
        /// <param name="payloadLength">The payload length in bytes.</param>
        /// <exception cref="ArgumentException">Thrown when the destination is too small.</exception>
        internal static void WriteSnapshotFileHeader(Span<byte> destination, int payloadLength)
        {
            if (destination.Length < GroupSnapshotCodec.SnapshotHeaderByteCount)
                throw new ArgumentException("Destination span is too small for the snapshot header.", nameof(destination));

            BinaryPrimitives.WriteUInt32LittleEndian(destination, GroupSnapshotCodec.SnapshotMagic);
            destination[GroupSnapshotCodec.SnapshotMagicByteCount] = GroupSnapshotCodec.SnapshotVersion;
            BinaryPrimitives.WriteUInt32LittleEndian(destination[GroupSnapshotCodec.SnapshotPayloadLengthOffset..], uint.CreateTruncating(payloadLength));
        }

        private static long UnixMs(DateTime utc)
        {
            var normalized = utc.Kind switch
            {
                DateTimeKind.Utc => utc,
                DateTimeKind.Local => utc.ToUniversalTime(),

                // Unspecified has no zone to convert; the established contract treats it as already-UTC
                // (relabel), matching prior behavior, so only Local wall-clock time is converted.
                _ => DateTime.SpecifyKind(utc, DateTimeKind.Utc),
            };
            return new DateTimeOffset(normalized).ToUnixTimeMilliseconds();
        }

        private static void WriteBytes(Span<byte> buffer, ReadOnlySpan<byte> value, int lengthPrefixByteCount, ref int offset)
        {
            if (lengthPrefixByteCount != sizeof(int))
                throw new ArgumentOutOfRangeException(nameof(lengthPrefixByteCount), lengthPrefixByteCount, "Snapshot byte fields use a 4-byte length prefix.");

            BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], value.Length);
            offset += lengthPrefixByteCount;
            value.CopyTo(buffer[offset..]);
            offset += value.Length;
        }

        private static void WriteOutcome(Span<byte> buffer, in GroupIdempotencyRecord record, ref int offset)
        {
            if (record.ResolvedUtc == null)
                throw new InvalidDataException("Snapshot outcome must be resolved.");
            WriteString16(buffer, record.OperationScope, ref offset);
            WriteString16(buffer, record.OperationId, ref offset);
            WriteBytes(buffer, record.OperationFingerprint.Span, GroupSnapshotCodec.OutcomeFingerprintLengthPrefixByteCount, ref offset);
            WriteBytes(buffer, record.OutcomePayload.Span, GroupSnapshotCodec.OutcomePayloadLengthPrefixByteCount, ref offset);
            var kindValue = GroupRecordKindCodec.ToWire(record.Kind);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], kindValue);
            offset += GroupSnapshotCodec.OutcomeKindByteCount;
            BinaryPrimitives.WriteInt64LittleEndian(buffer[offset..], UnixMs(record.CreatedUtc));
            offset += GroupSnapshotCodec.OutcomeCreatedUtcByteCount;
            buffer[offset] = 1;
            offset += GroupSnapshotCodec.OutcomeResolvedPresentByteCount;
            BinaryPrimitives.WriteInt64LittleEndian(buffer[offset..], UnixMs(record.ResolvedUtc.Value));
            offset += GroupSnapshotCodec.OutcomeResolvedUtcByteCount;
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], record.LogIndex);
            offset += GroupSnapshotCodec.OutcomeLogIndexByteCount;
            BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], record.Term);
            offset += GroupSnapshotCodec.OutcomeTermByteCount;
        }

        private static void WriteString(Span<byte> buffer, string value, ref int offset)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], byteCount);
            offset += GroupSnapshotCodec.GroupIdLengthPrefixByteCount;
            _ = Encoding.UTF8.GetBytes(value, buffer[offset..]);
            offset += byteCount;
        }

        private static void WriteString16(Span<byte> buffer, string value, ref int offset)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount > ushort.MaxValue)
                throw new ArgumentException($"String exceeds the maximum length of {ushort.MaxValue} UTF-8 bytes.", nameof(value));

            BinaryPrimitives.WriteUInt16LittleEndian(buffer[offset..], ushort.CreateChecked(byteCount));
            offset += GroupSnapshotCodec.String16LengthPrefixByteCount;
            _ = Encoding.UTF8.GetBytes(value, buffer[offset..]);
            offset += byteCount;
        }
    }

    /// <summary>Low-level snapshot payload decoding primitives.</summary>
    private static class GroupSnapshotPrimitiveReader
    {
        /// <summary>Highest Unix millisecond value <see cref="DateTimeOffset" /> accepts.</summary>
        private static readonly long MaxUnixMilliseconds = DateTimeOffset.MaxValue.ToUnixTimeMilliseconds();

        /// <summary>Lowest Unix millisecond value <see cref="DateTimeOffset" /> accepts.</summary>
        private static readonly long MinUnixMilliseconds = DateTimeOffset.MinValue.ToUnixTimeMilliseconds();

        internal static bool TryCreateUtc(long unixMs, out DateTime utc)
        {
            if (unixMs < MinUnixMilliseconds || unixMs > MaxUnixMilliseconds)
            {
                utc = default;
                return false;
            }

            utc = DateTimeOffset.FromUnixTimeMilliseconds(unixMs).UtcDateTime;
            return true;
        }

        internal static bool TryReadBytes(ReadOnlySpan<byte> buffer, int lengthPrefix, ref int offset, out ReadOnlyMemory<byte> value)
        {
            value = default;
            if (lengthPrefix != sizeof(int))
                return false;
            if (buffer.Length - offset < lengthPrefix)
                return false;

            var length = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
            offset += lengthPrefix;
            if (length < 0 || length > buffer.Length - offset)
                return false;

            value = OwnedBufferKit.CopyToOwned(buffer.Slice(offset, length));
            offset += length;
            return true;
        }

        internal static bool TryReadSnapshotHeader(ReadOnlySpan<byte> buffer, out SnapshotHeader header)
        {
            header = default;
            if (buffer.Length < GroupSnapshotCodec.SnapshotFixedByteCount + GroupSnapshotCodec.SnapshotPayloadPrefixByteCount)
                return false;

            var generation = BinaryPrimitives.ReadUInt64LittleEndian(buffer);
            const int fixedField = GroupSnapshotCodec.SnapshotFixedFieldByteCount;
            var lastIncludedTerm = BinaryPrimitives.ReadUInt64LittleEndian(buffer[fixedField..]);
            var lastIncludedIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[(fixedField * 2)..]);
            var commitIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[(fixedField * 3)..]);
            header = new SnapshotHeader(generation, lastIncludedTerm, lastIncludedIndex, commitIndex, GroupSnapshotCodec.SnapshotFixedByteCount);
            return true;
        }

        internal static bool TryReadString(ReadOnlySpan<byte> buffer, ref int offset, out string value)
        {
            value = string.Empty;
            if (buffer.Length - offset < GroupSnapshotCodec.GroupIdLengthPrefixByteCount)
                return false;

            var length = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.GroupIdLengthPrefixByteCount;
            if (length < 0 || length > buffer.Length - offset)
                return false;

            value = Encoding.UTF8.GetString(buffer.Slice(offset, length));
            offset += length;
            return true;
        }

        internal static bool TryReadString16(ReadOnlySpan<byte> buffer, ref int offset, out string value)
        {
            value = string.Empty;
            if (buffer.Length - offset < GroupSnapshotCodec.String16LengthPrefixByteCount)
                return false;

            var length = BinaryPrimitives.ReadUInt16LittleEndian(buffer[offset..]);
            offset += GroupSnapshotCodec.String16LengthPrefixByteCount;
            if (length > buffer.Length - offset)
                return false;

            value = Encoding.UTF8.GetString(buffer.Slice(offset, length));
            offset += length;
            return true;
        }

        /// <summary>Exact-size owned byte buffer helpers for replica-group encoding.</summary>
        private static class OwnedBufferKit
        {
#pragma warning disable ZA0302 // ZA0302: exact-size owned buffer escape; the caller retains ownership.
            internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
            {
                var owned = new byte[source.Length];
                source.CopyTo(owned);
                return owned;
            }
#pragma warning restore ZA0302
        }
    }
}
