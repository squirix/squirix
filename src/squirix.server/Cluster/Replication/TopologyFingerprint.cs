using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Canonical SHA-256 topology fingerprint with fixed-length byte comparison.</summary>
internal sealed class TopologyFingerprint : IEquatable<TopologyFingerprint>
{
    private readonly DigestBytes _digest;

    private TopologyFingerprint(ReadOnlySpan<byte> digest)
    {
        _digest = DigestBytes.FromSpan(digest);
    }

    /// <summary>Gets the 32-byte SHA-256 digest.</summary>
    internal ReadOnlySpan<byte> Bytes
    {
        get
        {
            ref var digest = ref Unsafe.AsRef(in _digest);
            return MemoryMarshal.CreateReadOnlySpan(ref Unsafe.As<DigestBytes, byte>(ref digest), 32);
        }
    }

    /// <inheritdoc />
    public bool Equals([NotNullWhen(true)] TopologyFingerprint? other) => other is not null && _digest.Equals(other._digest);

    /// <inheritdoc />
    public override bool Equals([NotNullWhen(true)] object? obj) => obj is TopologyFingerprint other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => BinaryPrimitives.ReadInt32LittleEndian(Bytes);

    /// <summary>Returns uppercase hex encoding of the digest.</summary>
    /// <returns>64-character uppercase hex string.</returns>
    public override string ToString() => Convert.ToHexString(Bytes);

    /// <summary>Computes the canonical topology fingerprint for a cluster configuration.</summary>
    /// <param name="topology">Cluster topology options.</param>
    /// <param name="mtlsOptions">Inter-node mTLS options used to derive effective peer URIs.</param>
    /// <returns>Canonical topology fingerprint.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> or <paramref name="mtlsOptions" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when required fingerprint input fields are empty.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the digest cannot be materialized.</exception>
    internal static TopologyFingerprint CreateFromTopology(TopologyOptions topology, MtlsOptions mtlsOptions)
    {
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(mtlsOptions);

        var peers = topology.Peers;
        var fingerprintPeers = new FingerprintPeer[peers.Length];
        var interNodeEnabled = MtlsTopology.RequiresInterNodeMtls(topology);
        for (var i = 0; i < peers.Length; i++)
        {
            var peer = peers[i];
            fingerprintPeers[i] = new FingerprintPeer(peer.NodeId, peer.Uri, ResolveInterNodeUri(peer, mtlsOptions, interNodeEnabled));
        }

        return Compute(
            new FingerprintInputs
            {
                ClusterId = topology.ClusterId,
                ConfigurationGeneration = topology.ConfigurationGeneration,
                ReplicaCount = topology.ReplicaCount,
                VirtualNodes = topology.VirtualNodes,
                Peers = fingerprintPeers,
                MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion,
                QuorumAckMode = PolicyOptions.QuorumAckMode,
            });
    }

    /// <summary>Computes a topology fingerprint from canonical inputs.</summary>
    /// <param name="inputs">Fingerprint inputs.</param>
    /// <returns>A fixed-length topology fingerprint.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="inputs" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when required input fields are empty.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the digest cannot be materialized.</exception>
    internal static TopologyFingerprint Compute(FingerprintInputs inputs)
    {
        ArgumentNullException.ThrowIfNull(inputs);
        ArgumentException.ThrowIfNullOrEmpty(inputs.ClusterId);
        ArgumentException.ThrowIfNullOrEmpty(inputs.MinClusterPackageVersion);
        ArgumentException.ThrowIfNullOrEmpty(inputs.QuorumAckMode);
        ArgumentNullException.ThrowIfNull(inputs.Peers);

        // Sort peers by ordinal node id / URI so peer list order never affects the digest.
        var peers = SortPeers(inputs.Peers);

        // Hash the closed policy vector first so format / capacity changes invalidate fingerprints.
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendInt32(hasher, inputs.CanonicalFormatVersion);
        AppendString(hasher, inputs.ClusterId);
        AppendInt32(hasher, inputs.ReplicaCount);
        AppendInt32(hasher, inputs.MaxReplicaCount);
        AppendInt32(hasher, inputs.VirtualNodes);
        AppendUInt64(hasher, inputs.ConfigurationGeneration);
        AppendString(hasher, inputs.MinClusterPackageVersion);

        // Append each peer as a length-prefixed UTF-8 tuple: node id, client URI, internode URI.
        AppendInt32(hasher, peers.Length);
        for (var i = 0; i < peers.Length; i++)
        {
            var peer = peers[i];
            AppendString(hasher, peer.NodeId);
            AppendString(hasher, peer.ClientUri.AbsoluteUri);
            AppendString(hasher, peer.InterNodeUri.AbsoluteUri);
        }

        // Finish with algorithm / durability / RF>1 policy constants that close the M8 contract.
        AppendInt32(hasher, inputs.HashAlgorithmVersion);
        AppendInt32(hasher, inputs.PlacementAlgorithmVersion);
        AppendInt32(hasher, inputs.ProtocolAlgorithmVersion);
        AppendInt32(hasher, inputs.DurabilitySchemaVersion);
        AppendString(hasher, inputs.QuorumAckMode);
        AppendInt32(hasher, inputs.RfIdempotencyMaxInFlightRecords);
        AppendInt64(hasher, inputs.RfIdempotencyRetentionTicks);
        AppendInt32(hasher, inputs.ClosedMessageMaxBytes);
        AppendInt32(hasher, inputs.ClosedSnapshotMaxBytes);

        Span<byte> digestSpan = stackalloc byte[32];
        if (!hasher.TryGetHashAndReset(digestSpan, out var written) || written is not 32)
            throw new InvalidOperationException("Failed to compute topology fingerprint digest.");

        return new TopologyFingerprint(digestSpan);
    }

    /// <summary>Derives a deterministic group id for an original owner under this fingerprint.</summary>
    /// <param name="clusterId">Cluster identifier.</param>
    /// <param name="originalOwnerNodeId">Original owner node identifier.</param>
    /// <returns>Uppercase hex group identifier.</returns>
    /// <exception cref="ArgumentException">Thrown when identifiers are empty.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the digest cannot be materialized.</exception>
    internal string CreateGroupId(string clusterId, string originalOwnerNodeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(clusterId);
        ArgumentException.ThrowIfNullOrEmpty(originalOwnerNodeId);

        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendString(hasher, "group-id-v1");
        AppendString(hasher, clusterId);
        AppendBytes(hasher, Bytes);
        AppendString(hasher, originalOwnerNodeId);
        Span<byte> digest = stackalloc byte[32];
        if (!hasher.TryGetHashAndReset(digest, out var written) || written is not 32)
            throw new InvalidOperationException("Failed to compute group id digest.");

        return Convert.ToHexString(digest);
    }

    private static void AppendBytes(IncrementalHash hasher, ReadOnlySpan<byte> value)
    {
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, value.Length);
        hasher.AppendData(length);
        hasher.AppendData(value);
    }

    private static void AppendInt32(IncrementalHash hasher, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        hasher.AppendData(buffer);
    }

    private static void AppendInt64(IncrementalHash hasher, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buffer, value);
        hasher.AppendData(buffer);
    }

    private static void AppendString(IncrementalHash hasher, string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, byteCount);
        hasher.AppendData(length);
        if (byteCount is 0)
            return;

        if (byteCount <= 512)
        {
            Span<byte> buffer = stackalloc byte[byteCount];
            _ = Encoding.UTF8.GetBytes(value, buffer);
            hasher.AppendData(buffer);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var written = Encoding.UTF8.GetBytes(value, rented.AsSpan(0, byteCount));
            hasher.AppendData(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void AppendUInt64(IncrementalHash hasher, ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, value);
        hasher.AppendData(buffer);
    }

    private static FingerprintPeer[] SortPeers(IReadOnlyList<FingerprintPeer> peers)
    {
        var copy = new FingerprintPeer[peers.Count];
        for (var i = 0; i < peers.Count; i++)
            copy[i] = peers[i];

        Array.Sort(
            copy,
            static (a, b) =>
            {
                var node = string.CompareOrdinal(a.NodeId, b.NodeId);
                if (node is not 0)
                    return node;

                var client = string.CompareOrdinal(a.ClientUri.AbsoluteUri, b.ClientUri.AbsoluteUri);
                return client is not 0 ? client : string.CompareOrdinal(a.InterNodeUri.AbsoluteUri, b.InterNodeUri.AbsoluteUri);
            });
        return copy;
    }

    private static Uri ResolveInterNodeUri(ServerPeer peer, MtlsOptions mtlsOptions, bool interNodeEnabled)
    {
        if (!interNodeEnabled)
            return peer.Uri;

        if (peer.InterNodeUri is { } configured)
            return configured;

        if (mtlsOptions.InternalListenPort <= 0)
            return peer.Uri;

        return new UriBuilder(peer.Uri.Scheme, peer.Uri.Host, mtlsOptions.InternalListenPort).Uri;
    }

    /// <summary>Inline 32-byte SHA-256 digest storage (readonly for NDepend ND1914; avoids InlineArray CS9180/CS8340 conflict).</summary>
    [StructLayout(LayoutKind.Sequential, Size = 32)]
    private readonly struct DigestBytes : IEquatable<DigestBytes>
    {
        private readonly ulong _w0;
        private readonly ulong _w1;
        private readonly ulong _w2;
        private readonly ulong _w3;

        private DigestBytes(ulong w0, ulong w1, ulong w2, ulong w3)
        {
            _w0 = w0;
            _w1 = w1;
            _w2 = w2;
            _w3 = w3;
        }

        /// <inheritdoc />
        public bool Equals(DigestBytes other) => _w0 == other._w0 && _w1 == other._w1 && _w2 == other._w2 && _w3 == other._w3;

        /// <inheritdoc />
        public override bool Equals([NotNullWhen(true)] object? obj) => obj is DigestBytes other && Equals(other);

        /// <inheritdoc />
        public override int GetHashCode() => HashCode.Combine(_w0, _w1, _w2, _w3);

        internal static DigestBytes FromSpan(ReadOnlySpan<byte> digest)
        {
            if (digest.Length is not 32)
                throw new ArgumentException("Digest must be exactly 32 bytes.", nameof(digest));

            return new DigestBytes(
                BinaryPrimitives.ReadUInt64LittleEndian(digest),
                BinaryPrimitives.ReadUInt64LittleEndian(digest[8..]),
                BinaryPrimitives.ReadUInt64LittleEndian(digest[16..]),
                BinaryPrimitives.ReadUInt64LittleEndian(digest[24..]));
        }
    }
}
