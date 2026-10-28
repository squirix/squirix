using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Replica operation fingerprints hash the serialized request, whatever its size, exactly as if the serialized bytes were hashed in one piece.</summary>
[Immutable]
public sealed class ReplicaOperationFingerprintsTests : ServerUnitTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private const string CacheName = "cache";
    private const string Key = "key";

    /// <summary>A set and an add hash the same bytes as the serialized entry at every size around the stack and chunk boundaries.</summary>
    /// <param name="valueLength">The length of the string value.</param>
    [Test]
    [Arguments(0)]
    [Arguments(100)]
    [Arguments(500)]
    [Arguments(520)]
    [Arguments(IncrementalHashBufferWriter.ChunkBytes)]
    [Arguments(IncrementalHashBufferWriter.ChunkBytes + 100)]
    [Arguments(200_000)]
    public async Task EntryFingerprintsMatchSerializedBytes(int valueLength)
    {
        var entry = new NodeCacheEntry<object?> { Value = new string('x', valueLength), Version = 1, Expiration = TimeSpan.FromMinutes(1) };
        var bytes = entry.MapToProto().ToByteArray();

        var set = ReplicaOperationFingerprints.Set(OperationId, CacheName, Key, entry);
        var add = ReplicaOperationFingerprints.AddIfAbsent(OperationId, CacheName, Key, entry);

        await SequenceAssert.EqualAsync(Oracle(ReplicaMutationKinds.Set, bytes), set);
        await SequenceAssert.EqualAsync(Oracle(ReplicaMutationKinds.TryAdd, bytes), add);
    }

    /// <summary>An update hashes the same bytes as the serialized value, for a scalar and for a JSON value of any size.</summary>
    /// <param name="jsonLength">The number of array items of the JSON value; zero means a scalar.</param>
    [Test]
    [Arguments(0)]
    [Arguments(10)]
    [Arguments(10_000)]
    public async Task UpdateFingerprintMatchesSerializedBytes(int jsonLength)
    {
        object? value = jsonLength == 0 ? 42L : JsonValueKit.Create(ArrayOfOnes(jsonLength));
        var bytes = ServerProtoEx.CacheValueToGrpcValue(value).ToByteArray();

        var fingerprint = ReplicaOperationFingerprints.Update(OperationId, CacheName, Key, value);

        await SequenceAssert.EqualAsync(Oracle(ReplicaMutationKinds.Update, bytes), fingerprint);
    }

    /// <summary>The same request has the same fingerprint, and another value has another.</summary>
    [Test]
    public async Task FingerprintTellsRequestsApart()
    {
        var first = new NodeCacheEntry<object?> { Value = "a", Version = 1 };
        var second = new NodeCacheEntry<object?> { Value = "b", Version = 1 };

        await SequenceAssert.EqualAsync(ReplicaOperationFingerprints.Set(OperationId, CacheName, Key, first), ReplicaOperationFingerprints.Set(OperationId, CacheName, Key, first));
        _ = await Assert.That(Convert.ToHexString(ReplicaOperationFingerprints.Set(OperationId, CacheName, Key, first)))
                        .IsNotEqualTo(Convert.ToHexString(ReplicaOperationFingerprints.Set(OperationId, CacheName, Key, second)));
    }

    private static string ArrayOfOnes(int count)
    {
        var builder = new StringBuilder("[");
        for (var i = 0; i < count; i++)
            _ = builder.Append("1,");

        return builder.Append("0]").ToString();
    }

    private static byte[] Oracle(string kind, byte[] payload)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendField(hash, CacheName);
        AppendField(hash, OperationId);
        AppendField(hash, CacheName);
        AppendField(hash, Key);
        AppendField(hash, kind);
        hash.AppendData(payload);
        return hash.GetHashAndReset();
    }

    private static void AppendField(IncrementalHash hash, string field)
    {
        var bytes = Encoding.UTF8.GetBytes(field);
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }
}
