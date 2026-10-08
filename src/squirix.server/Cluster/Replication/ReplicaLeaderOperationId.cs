using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Derives the deterministic identity of the no-op a leader commits at the start of its term.</summary>
/// <remarks>
/// One term has one leader, so the term alone names the entry within its group: a retry of the no-op in the same term keeps its
/// identity and replays the retained entry instead of appending a second one.
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

    /// <summary>Creates the operation identifier of the no-op of a term.</summary>
    /// <param name="term">The leader term.</param>
    /// <returns>The identifier: <c language="text">term-</c> followed by the term in invariant decimal digits.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="term" /> is zero.</exception>
    internal static string Create(ulong term)
    {
        ArgumentOutOfRangeException.ThrowIfZero(term);
        return $"term-{term}";
    }

    /// <summary>Computes the operation fingerprint of the no-op of a term in a group.</summary>
    /// <param name="groupId">The replica group identifier.</param>
    /// <param name="term">The leader term.</param>
    /// <returns>The SHA-256 digest over the domain, the group identifier and the term.</returns>
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
