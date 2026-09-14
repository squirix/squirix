using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Prepares replicated cache mutations with preset exact outcomes.</summary>
/// <remarks>
/// Outcome presets come from prepare-time reads. Commits run serialized per group, so no interleaving
/// mutation can invalidate a preset between the read and the ordered apply: the preset always matches.
/// </remarks>
[Immutable]
internal sealed class ReplicaMutationFactory
{
    private readonly string _groupId;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly ulong _term;

    /// <summary>Initializes a new instance of the <see cref="ReplicaMutationFactory" /> class.</summary>
    /// <param name="local">Local cache pipeline used for prepare-time reads and follower applies.</param>
    /// <param name="groupId">Owned replica group identifier.</param>
    /// <param name="term">Static leader term for prepared mutations.</param>
    internal ReplicaMutationFactory(ILogicalNamespacedCache<object?> local, string groupId, ulong term)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentOutOfRangeException.ThrowIfZero(term);
        _local = local;
        _groupId = groupId;
        _term = term;
    }

    /// <summary>Prepares a replicated remove with the observed previous entry as the outcome.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared mutation.</returns>
    internal async Task<PreparedReplicaMutation> PrepareRemoveAsync(string operationId, string cacheName, string key, ulong index, CancellationToken cancellationToken)
    {
        var previous = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var outcome = previous == null ? ReplicaOutcomeCodec.Encode(false, ReadOnlyMemory<byte>.Empty) : ReplicaOutcomeCodec.Encode(true, previous.MapToProto().ToByteArray());
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
            ReadOnlyMemory<byte>.Empty,
            ReadOnlyMemory<byte>.Empty,
            0,
            DateTime.UtcNow.Ticks,
            0,
            0);
        return Build(cacheName, in record, outcome, index);
    }

    /// <summary>Prepares a replicated expiration removal.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="index">Reserved group log index.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared mutation.</returns>
    internal async Task<PreparedReplicaMutation> PrepareRemoveExpirationAsync(string operationId, string cacheName, string key, ulong index, CancellationToken cancellationToken)
    {
        var current = await _local.GetEntryAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        var outcome = ReplicaOutcomeCodec.Encode(current?.ExpiresUtc != null, ReadOnlyMemory<byte>.Empty);
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            ReplicaExpirationOperationId.OperationScope,
            Fingerprint(ReplicaExpirationOperationId.OperationScope, operationId, cacheName, key, ReplicaMutationKinds.RemoveExpiration, []),
            nameof(GroupRecordKind.Expiration),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.RemoveExpiration,
            ReadOnlyMemory<byte>.Empty,
            ReadOnlyMemory<byte>.Empty,
            0,
            DateTime.UtcNow.Ticks,
            0,
            0);
        return Build(ReplicaExpirationOperationId.OperationScope, in record, outcome, index);
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
        var mutation = entry.MapToProto().ToByteArray();
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.Set, mutation),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.Set,
            mutation,
            ReadOnlyMemory<byte>.Empty,
            entry.ExpiresUtc?.Ticks ?? 0,
            DateTime.UtcNow.Ticks,
            0,
            0);
        return Build(cacheName, in record, ReplicaOutcomeCodec.Encode(true, ReadOnlyMemory<byte>.Empty), index);
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
    /// The Touch wire representation carries the expiration as a <see cref="TimeSpan" /> duration in
    /// ticks inside <c language="csharp">ExpiresUtcTicks</c>, matching the <see cref="TimeSpan" /> conversion
    /// in the follower applier. All other mutation kinds carry an absolute UTC timestamp there instead;
    /// consumers must branch on the mutation kind and never interpret a Touch duration as a timestamp.
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
        var outcome = ReplicaOutcomeCodec.Encode(current != null, ReadOnlyMemory<byte>.Empty);
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
            ReadOnlyMemory<byte>.Empty,
            ReadOnlyMemory<byte>.Empty,
            expiration.Ticks,
            DateTime.UtcNow.Ticks,
            0,
            0);
        return Build(cacheName, in record, outcome, index);
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
        var outcome = ReplicaOutcomeCodec.Encode(current == null, ReadOnlyMemory<byte>.Empty);
        var mutation = entry.MapToProto().ToByteArray();
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.TryAdd, mutation),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.TryAdd,
            mutation,
            ReadOnlyMemory<byte>.Empty,
            entry.ExpiresUtc?.Ticks ?? 0,
            DateTime.UtcNow.Ticks,
            0,
            0);
        return Build(cacheName, in record, outcome, index);
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
        var outcome = ReplicaOutcomeCodec.Encode(current != null, ReadOnlyMemory<byte>.Empty);
        var mutation = ServerProtoEx.CacheValueToGrpcValue(value).ToByteArray();
        var record = new ReplicaLogRecord(
            index,
            _term,
            operationId,
            cacheName,
            Fingerprint(cacheName, operationId, cacheName, key, ReplicaMutationKinds.Update, mutation),
            nameof(GroupRecordKind.UserMutation),
            cacheName,
            Encoding.UTF8.GetBytes(key),
            ReplicaMutationKinds.Update,
            mutation,
            ReadOnlyMemory<byte>.Empty,
            current?.ExpiresUtc?.Ticks ?? 0,
            DateTime.UtcNow.Ticks,
            0,
            0);
        return Build(cacheName, in record, outcome, index);
    }

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

    private PreparedReplicaMutation Build(string scope, in ReplicaLogRecord record, byte[] outcome, ulong index)
    {
        var canonical = ReplicaLogCodec.Encode(in record);
        var identity = new ReplicaOperationIdentity(_groupId, scope, record.OperationId, record.OperationFingerprint);
        var payload = new ReplicaMutationPayload(canonical, outcome, Crc32C.Compute(canonical));
        return new PreparedReplicaMutation(identity, _term, index, payload);
    }
}
