using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization.Grpc;
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

    /// <summary>Every mutation preserves the UTF-8 field order, including long and non-ASCII fields.</summary>
    /// <param name="keyLength">The key length around the stack buffer boundary.</param>
    [Test]
    [Arguments(0)]
    [Arguments(100)]
    [Arguments(500)]
    [Arguments(4096)]
    public async Task AllKindsKeepCanonicalFields(int keyLength)
    {
        const string operationId = "операция-😀";
        const string cacheName = "кэш-😀";
        var key = new string('я', keyLength);
        var entry = new NodeCacheEntry<object?>("значение-😀", 17);
        var expiration = TimeSpan.FromTicks(123456789);
        var touchPayload = GC.AllocateUninitializedArray<byte>(sizeof(long));
        BinaryPrimitives.WriteInt64LittleEndian(touchPayload, expiration.Ticks);
        var expiredPayload = GC.AllocateUninitializedArray<byte>(sizeof(long) * 2);
        BinaryPrimitives.WriteInt64LittleEndian(expiredPayload, entry.Version);
        BinaryPrimitives.WriteInt64LittleEndian(expiredPayload.AsSpan(sizeof(long)), 987654321L);

        await SequenceAssert.EqualAsync(
            Oracle(ReplicaMutationKinds.Set, entry.MapToProto().ToByteArray(), operationId, cacheName, key),
            ReplicaOperationFingerprints.Set(operationId, cacheName, key, entry));
        await SequenceAssert.EqualAsync(
            Oracle(ReplicaMutationKinds.TryAdd, entry.MapToProto().ToByteArray(), operationId, cacheName, key),
            ReplicaOperationFingerprints.AddIfAbsent(operationId, cacheName, key, entry));
        await SequenceAssert.EqualAsync(
            Oracle(ReplicaMutationKinds.Update, ServerProtoEx.CacheValueToGrpcValue<object?>(null).ToByteArray(), operationId, cacheName, key),
            ReplicaOperationFingerprints.Update(operationId, cacheName, key, null));
        await SequenceAssert.EqualAsync(
            Oracle(ReplicaMutationKinds.Remove, [], operationId, cacheName, key),
            ReplicaOperationFingerprints.Remove(operationId, cacheName, key));
        await SequenceAssert.EqualAsync(
            Oracle(ReplicaMutationKinds.RemoveExpiration, [], operationId, cacheName, key),
            ReplicaOperationFingerprints.RemoveExpiration(operationId, cacheName, key));
        await SequenceAssert.EqualAsync(
            Oracle(ReplicaMutationKinds.Touch, touchPayload, operationId, cacheName, key),
            ReplicaOperationFingerprints.Touch(operationId, cacheName, key, expiration));
        await SequenceAssert.EqualAsync(
            Oracle(ReplicaMutationKinds.Expire, expiredPayload, string.Empty, cacheName, key),
            ReplicaOperationFingerprints.Expire(cacheName, key, entry.Version, 987654321L));
    }

    private static string ArrayOfOnes(int count)
    {
        var builder = new StringBuilder("[");
        for (var i = 0; i < count; i++)
            _ = builder.Append("1,");

        return builder.Append("0]").ToString();
    }

    private static byte[] Oracle(string kind, byte[] payload, string operationId = OperationId, string cacheName = CacheName, string key = Key)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendField(hash, cacheName);
        AppendField(hash, operationId);
        AppendField(hash, cacheName);
        AppendField(hash, key);
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
