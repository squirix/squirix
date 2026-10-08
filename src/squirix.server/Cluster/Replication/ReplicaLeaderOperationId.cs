using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Derives the deterministic identity of the no-op a leader appends at the start of its leadership.</summary>
/// <remarks>
/// One term has one leader and one log index holds one entry, so the term and the index name the entry within its group. Every
/// leadership appends a no-op of its own: a leader of the provisional term one that restarts appends another at a new index, which no
/// entry of the earlier run can stand in for, and two no-ops of one term recovered together in a tail never share a pin.
/// </remarks>
internal static class ReplicaLeaderOperationId
{
    /// <summary>The operation scope reserved for leader-term no-ops.</summary>
    /// <remarks>
    /// Client mutations use their cache name as the scope. A cache name cannot contain a colon, so no client operation can occupy this
    /// scope or collide with a leader-term identity.
    /// </remarks>
    internal const string OperationScope = "squirix:leader-term";

    private const string Domain = "squirix:leader-term:v1";

    /// <summary>Creates the operation identifier of the no-op of a term at a log index.</summary>
    /// <param name="term">The leader term.</param>
    /// <param name="logIndex">The log index of the no-op.</param>
    /// <returns>The identifier: <c language="text">term-</c>, the term, a dash, and the log index, both in decimal digits.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="term" /> or <paramref name="logIndex" /> is zero.</exception>
    internal static string Create(ulong term, ulong logIndex)
    {
        ArgumentOutOfRangeException.ThrowIfZero(term);
        ArgumentOutOfRangeException.ThrowIfZero(logIndex);
        return $"term-{term}-{logIndex}";
    }

    /// <summary>Computes the operation fingerprint of the no-op of a term in a group.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="term">The leader term.</param>
    /// <returns>The SHA-256 digest over the domain, the group identifier and the term.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException"><paramref name="groupId" /> is empty or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="term" /> is zero.</exception>
    internal static byte[] Fingerprint(string groupId, ulong term)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentOutOfRangeException.ThrowIfZero(term);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendUtf8(hash, Domain);
        AppendUtf8(hash, groupId);
        Span<byte> termBytes = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(termBytes, term);
        AppendBytes(hash, termBytes);
        return hash.GetHashAndReset();
    }

    private static void AppendUtf8(IncrementalHash hash, string value) => AppendBytes(hash, Encoding.UTF8.GetBytes(value));

    private static void AppendBytes(IncrementalHash hash, ReadOnlySpan<byte> value)
    {
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, value.Length);
        hash.AppendData(length);
        hash.AppendData(value);
    }
}
