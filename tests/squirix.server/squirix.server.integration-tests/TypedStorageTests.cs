using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies the value contract through the public binary transport and a real node.</summary>
public sealed class TypedStorageTests : NodeIntegrationTestBase
{
    /// <summary>The same key can change its kind. A stored null and an absent key have different read results.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplacementPreservesWireKindAndBytes(CancellationToken cancellationToken)
    {
        const string nodeId = "typed-node";
        const string cacheName = "mixed";
        const string key = "record";
        await using var cluster = await StartClusterAsync(nodeId, new IntegrationStartOptions(), cancellationToken);
        using var channel = CreateBinaryChannel(cluster[nodeId].Uri);
        var client = new CacheWireClient(channel);
        var query = new GetValueAsyncRequest { CacheName = cacheName, Key = key };
        var missing = await client.GetValueAsync(query, cancellationToken: cancellationToken);
        _ = await Assert.That(missing.Found).IsFalse();
        CacheValue[] values =
        [
            new() { BoolValue = false },
            new() { BoolValue = true },
            new() { Int64Value = 0 },
            new() { Int64Value = long.MinValue },
            new() { DoubleValue = -0d },
            new() { StringValue = "Ж漢😀" },
            new() { BinaryValue = new byte[] { 0, 255 } },
            new() { JsonUtf8 = System.Text.Encoding.UTF8.GetBytes("""{ "n":12345678901234567890,"n":2 }""") },
            new() { StringValue = string.Empty },
            new() { BinaryValue = ReadOnlyMemory<byte>.Empty },
            CacheValueWire.Null(),
        ];

        foreach (var value in values)
        {
            var set = new SetEntryAsyncRequest { CacheName = cacheName, Key = key, Entry = new CacheEntryWire { Value = value } };
            _ = await client.SetEntryAsync(set, cancellationToken: cancellationToken);
            var hit = await client.GetValueAsync(query, cancellationToken: cancellationToken);
            _ = await Assert.That(hit.Found).IsTrue();
            _ = await Assert.That(hit.Value!.KindCase).IsEqualTo(value.KindCase);
            _ = await Assert.That(hit.Value.ToByteArray().AsSpan().SequenceEqual(value.ToByteArray())).IsTrue();
        }
    }
}
