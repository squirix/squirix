using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Prepares replicated cache mutations that carry the outcome and the effect the leader decided.</summary>
/// <remarks>
/// Each mutation is decided once, from one prepare-time read of the key and one reading of the clock: the outcome, the effect and the
/// pinned deadline all come from that decision and travel in the record. Applying the record executes the decision without reading
/// a clock or the cache again, so nothing after prepare can change what the client was told. Commits run serialized per group and no
/// prepare runs while an appended entry is unapplied, so the decision always sees every earlier effect. The factory also rebuilds
/// recovered uncommitted log entries, taking their outcome from the record.
/// </remarks>
[Immutable]
internal sealed class ReplicaMutationFactory : IReplicaTailRebuilder
{
    private readonly TimeProvider _clock;
    private readonly string _groupId;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly ILogger _log;
    private readonly ulong _term;

    /// <summary>Initializes a new instance of the <see cref="ReplicaMutationFactory" /> class.</summary>
    /// <param name="local">Local cache pipeline used for prepare-time reads and follower applies.</param>
    /// <param name="groupId">Owned replica group identifier.</param>
    /// <param name="term">Static leader term for prepared mutations.</param>
    /// <param name="clock">Time source that pins the absolute expiration deadlines of prepared records.</param>
    /// <param name="log">Logger of the records found inconsistent at prepare.</param>
    internal ReplicaMutationFactory(ILogicalNamespacedCache<object?> local, string groupId, ulong term, TimeProvider clock, ILogger log)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentOutOfRangeException.ThrowIfZero(term);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(log);
        _local = local;
        _groupId = groupId;
        _term = term;
        _clock = clock;
        _log = log;
    }

    /// <inheritdoc />
    public PreparedReplicaMutation Rebuild(FollowerLogEntry entry)
    {
        var record = DecodeRecord(entry.Payload, entry.LogIndex);
        if (record.LogIndex != entry.LogIndex || record.Term != entry.Term)
            throw new InvalidDataException($"Replica log entry {entry.LogIndex} carries the record of another position.");

        // The record carries the decided outcome and effect; a record whose two disagree is refused instead of rebuilt.
        _ = ReplicaCacheApplier.ResolveEffect(in record);
        var identity = new ReplicaOperationIdentity(_groupId, record.OperationScope, record.OperationId, record.OperationFingerprint);
        var payload = new ReplicaMutationPayload(entry.Payload, record.OutcomePayload, Crc32C.Compute(entry.PayloadSpan));
        return new PreparedReplicaMutation(identity, entry.Term, entry.LogIndex, payload);
    }

    /// <summary>Prepares a replicated remove with the observed previous entry as the outcome.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared mutation.</returns>
    /// <remarks>The record deletes the key whatever the leader observed: a key the leader found absent or expired may still be held by a replica.</remarks>
    internal async Task<PreparedReplicaMutation> PrepareRemoveAsync(string operationId, string cacheName, string key, ulong index, CancellationToken cancellationToken)
    {
        var current = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var now = _clock.GetUtcNow().UtcDateTime;
        var decision = ReplicaMutationDecisions.DecideRemove(current);
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.Remove, []),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.Remove,
            decision.Payload,
            decision.Outcome,
            decision.ExpiresUtcTicks,
            now.Ticks,
            0,
            0);
        return Build(cacheName, in record, index);
    }

    /// <summary>Prepares a replicated expiration removal, a client mutation scoped to its cache like every other one.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared mutation.</returns>
    internal async Task<PreparedReplicaMutation> PrepareRemoveExpirationAsync(string operationId, string cacheName, string key, ulong index, CancellationToken cancellationToken)
    {
        var current = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var now = _clock.GetUtcNow().UtcDateTime;
        var decision = ReplicaMutationDecisions.DecideRemoveExpiration(current);
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.RemoveExpiration, []),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.RemoveExpiration,
            decision.Payload,
            decision.Outcome,
            decision.ExpiresUtcTicks,
            now.Ticks,
            0,
            0);
        return Build(cacheName, in record, index);
    }

    /// <summary>Prepares a replicated unconditional write.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="entry">Entry to write.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <returns>The prepared mutation.</returns>
    internal PreparedReplicaMutation PrepareSet(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, ulong index)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var request = entry.MapToProto().ToByteArray();
        var now = _clock.GetUtcNow().UtcDateTime;
        var decision = ReplicaMutationDecisions.DecideSet(entry, now);
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.Set, request),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.Set,
            decision.Payload,
            decision.Outcome,
            decision.ExpiresUtcTicks,
            now.Ticks,
            0,
            0);
        return Build(cacheName, in record, index);
    }

    /// <summary>Prepares a replicated conditional expiration refresh.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="expiration">New expiration.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared mutation.</returns>
    /// <remarks>
    /// The record carries the touched entry and pins its absolute deadline, prepare time plus <paramref name="expiration" />, in
    /// <c language="csharp">ExpiresUtcTicks</c>, so every apply of it (a replay included) sets the same deadline instead of
    /// extending it from the apply time. The fingerprint does not cover the deadline: a retry prepared at a later time keeps
    /// its operation identity.
    /// </remarks>
    internal async Task<PreparedReplicaMutation> PrepareTouchAsync(
        string operationId,
        string cacheName,
        string key,
        TimeSpan expiration,
        ulong index,
        CancellationToken cancellationToken)
    {
        var current = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var now = _clock.GetUtcNow().UtcDateTime;
        var decision = ReplicaMutationDecisions.DecideTouch(current, now, expiration);
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.Touch, []),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.Touch,
            decision.Payload,
            decision.Outcome,
            decision.ExpiresUtcTicks,
            now.Ticks,
            0,
            0);
        return Build(cacheName, in record, index);
    }

    /// <summary>Prepares a replicated conditional add.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="entry">Entry to add when absent.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared mutation.</returns>
    internal async Task<PreparedReplicaMutation> PrepareTryAddAsync(
        string operationId,
        string cacheName,
        string key,
        NodeCacheEntry<object?> entry,
        ulong index,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var current = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var request = entry.MapToProto().ToByteArray();
        var now = _clock.GetUtcNow().UtcDateTime;
        var decision = ReplicaMutationDecisions.DecideTryAdd(current, entry, now);
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.TryAdd, request),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.TryAdd,
            decision.Payload,
            decision.Outcome,
            decision.ExpiresUtcTicks,
            now.Ticks,
            0,
            0);
        return Build(cacheName, in record, index);
    }

    /// <summary>Prepares a replicated value replacement.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="value">Replacement value.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared mutation.</returns>
    internal async Task<PreparedReplicaMutation> PrepareUpdateAsync(
        string operationId,
        string cacheName,
        string key,
        object? value,
        ulong index,
        CancellationToken cancellationToken)
    {
        var current = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var request = ServerProtoEx.CacheValueToGrpcValue(value).ToByteArray();
        var now = _clock.GetUtcNow().UtcDateTime;
        var decision = ReplicaMutationDecisions.DecideUpdate(current, value);
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.Update, request),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.Update,
            decision.Payload,
            decision.Outcome,
            decision.ExpiresUtcTicks,
            now.Ticks,
            0,
            0);
        return Build(cacheName, in record, index);
    }

    private static ReplicaLogRecord DecodeRecord(ReadOnlyMemory<byte> payload, ulong logIndex) =>
        ReplicaLogCodec.Decode(payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidDataException($"Replica log entry {logIndex} carries an undecodable canonical payload."));

    private static void Append(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }

    /// <summary>Computes the canonical operation fingerprint.</summary>
    /// <param name="scope">Operation scope distinguishing user mutations from expirations.</param>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="mutationKind">Cache mutation kind.</param>
    /// <param name="mutationPayload">Cache mutation bytes.</param>
    /// <returns>The SHA-256 fingerprint bytes.</returns>
    /// <remarks>
    /// Each string field contributes its 32-bit little-endian UTF-8 byte count followed by the bytes
    /// themselves, in field order, so concatenations that only differ at field boundaries hash
    /// differently.
    /// </remarks>
    private static byte[] Fingerprint(string scope, string operationId, string cacheName, string key, string mutationKind, ReadOnlySpan<byte> mutationPayload)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, scope);
        Append(hash, operationId);
        Append(hash, cacheName);
        Append(hash, key);
        Append(hash, mutationKind);
        hash.AppendData(mutationPayload);
        return hash.GetHashAndReset();
    }

    /// <summary>Encodes a decided record after checking that its effect agrees with its outcome.</summary>
    /// <remarks>Only the shape is checked: the entry the decision just encoded is not decoded again on every write.</remarks>
    /// <param name="scope">Operation scope.</param>
    /// <param name="record">The decided record.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <returns>The prepared mutation, carrying the outcome of the record.</returns>
    /// <exception cref="InvalidDataException">The record is inconsistent, which is a defect of the decision; nothing was appended yet.</exception>
    private PreparedReplicaMutation Build(string scope, in ReplicaLogRecord record, ulong index)
    {
        try
        {
            _ = ReplicaCacheApplier.ResolveShape(in record);
        }
        catch (InvalidDataException error)
        {
            ServerLog.ReplicaInconsistentDecision(_log, _groupId, error);
            throw;
        }

        var outcome = record.OutcomePayload;
        var canonical = ReplicaLogCodec.Encode(in record);
        var identity = new ReplicaOperationIdentity(_groupId, scope, record.OperationId, record.OperationFingerprint);
        var payload = new ReplicaMutationPayload(canonical, outcome, Crc32C.Compute(canonical));
        return new PreparedReplicaMutation(identity, _term, index, payload);
    }
}
