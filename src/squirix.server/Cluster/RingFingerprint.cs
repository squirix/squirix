using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

/// <summary>Digest of the inputs that decide key ownership on the RF=1 consistent-hash ring.</summary>
/// <remarks>
/// Two nodes build the same ring only when their cluster id, virtual node count, and distinct node id set match, so equal fingerprints mean equal ownership.
/// Peer order, duplicate ids, URIs, configuration generation, and replica count do not affect it.
/// </remarks>
[Immutable]
internal sealed class RingFingerprint
{
    private const int FormatVersion = 1;

    private RingFingerprint(string value)
    {
        Value = value;
    }

    /// <summary>Gets the digest as 64 uppercase hex characters.</summary>
    internal string Value { get; }

    /// <summary>Computes the fingerprint of a ring configuration.</summary>
    /// <param name="clusterId">Cluster identifier.</param>
    /// <param name="nodeIds">Configured peer node identifiers; order and duplicates are ignored.</param>
    /// <param name="virtualNodes">Virtual nodes per physical node.</param>
    /// <returns>The ring fingerprint.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the digest cannot be materialized.</exception>
    internal static RingFingerprint Create(string clusterId, ReadOnlySpan<string> nodeIds, int virtualNodes)
    {
        ArgumentNullException.ThrowIfNull(clusterId);

        var distinct = DistinctNodeIds.InInsertionOrder(nodeIds);
        Array.Sort(distinct, StringComparer.Ordinal);

        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendInt32(hasher, FormatVersion);
        AppendString(hasher, clusterId);
        AppendInt32(hasher, virtualNodes);
        AppendInt32(hasher, distinct.Length);
        for (var i = 0; i < distinct.Length; i++)
            AppendString(hasher, distinct[i]);

        Span<byte> digest = stackalloc byte[32];
        return hasher.TryGetHashAndReset(digest, out var written) && written == digest.Length
            ? new RingFingerprint(Convert.ToHexString(digest))
            : throw new InvalidOperationException("Failed to compute ring fingerprint digest.");
    }

    private static void AppendInt32(IncrementalHash hasher, int value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        hasher.AppendData(buffer);
    }

    private static void AppendString(IncrementalHash hasher, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        AppendInt32(hasher, bytes.Length);
        hasher.AppendData(bytes);
    }
}
