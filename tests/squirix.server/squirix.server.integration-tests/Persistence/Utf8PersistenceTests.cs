using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Persistence;

/// <summary>Checks UTF-8 ownership through a persistent node and typed restoration from snapshot files.</summary>
public sealed class Utf8PersistenceTests : NodeIntegrationTestBase
{
    /// <summary>Loading text as a string allocates the decoded value without copying its UTF-8 payload.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TypedSnapshotLoadAvoidsPayloadCopy(CancellationToken cancellationToken)
    {
        var value = new string('x', 32 * 1024);
        var key = CacheKey.Default("text");
        using var directory = new TempDirectory("typed-snapshot");
        var path = await new SnapshotWriter(directory).WriteAsync(1, [(key, new NodeCacheEntry<object?>(value, 7))], [], cancellationToken);
        var reader = StoreFactory.CreateReader(64 * 1024);
        _ = await reader.LoadStrictAsync<string>(path, null, cancellationToken);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var loaded = await reader.LoadStrictAsync<string>(path, null, cancellationToken);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        _ = await Assert.That(loaded.Entries.Count).IsEqualTo(1);
        _ = await Assert.That(loaded.Entries[0].Key).IsEqualTo(key);
        _ = await Assert.That(loaded.Entries[0].Entry.Value).IsEqualTo(value);
        _ = await Assert.That(loaded.Entries[0].Entry.Version).IsEqualTo(7L);
        _ = await Assert.That(allocated).IsLessThan(72L * 1024);
    }

    /// <summary>A well-formed snapshot with an incompatible live value retains its payload error.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TypedSnapshotRejectsIncompatibleValue(CancellationToken cancellationToken)
    {
        var key = CacheKey.Default("number");
        using var directory = new TempDirectory("typed-snapshot");
        var path = await new SnapshotWriter(directory).WriteAsync(1, [(key, new NodeCacheEntry<object?>(42L))], [], cancellationToken);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws((path, cancellationToken), static s => _ = StoreFactory.CreateReader().LoadStrictAsync<string>(s.path, null, s.cancellationToken).AsTask());

        _ = await Assert.That(failure.Message).IsEqualTo("Binary snapshot entry payload could not be read.");
    }

    /// <summary>A routed text update preserves the parsed payload through journaling and storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RoutedUpdatePreservesUtf8Payload(CancellationToken cancellationToken)
    {
        const string nodeId = "node-a";
        const string cacheName = "default";
        const string key = "text";
        const string replacement = "Ж漢😀";
        await using var cluster = await StartClusterAsync(nodeId, new IntegrationStartOptions { UsePersistence = true }, cancellationToken);
        var node = cluster[nodeId];
        var mutations = node.GetRequiredService<RoutedCacheOperations<object?>>().Mutations;
        var initial = new SetEntryAsyncRequest
        {
            CacheName = cacheName,
            Key = key,
            OperationId = Guid.NewGuid().ToString("N"),
            Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "first" } },
        };
        _ = await mutations.SetEntryAsync(initial, cancellationToken);
        var request = new UpdateAsyncRequest
        {
            CacheName = cacheName,
            Key = key,
            OperationId = Guid.NewGuid().ToString("N"),
            Entry = new CacheEntryWire { Value = new CacheValue { StringValue = replacement } },
        };
        var parsed = UpdateAsyncRequest.Parser.ParseFrom(request.ToByteArray());
        var bytes = parsed.Entry!.Value!.EncodedText;
        _ = await Assert.That(bytes).IsNotNull();

        var response = await mutations.UpdateAsync(parsed, cancellationToken);

        _ = await Assert.That(response.Updated).IsTrue();
        var hit = node.GetRequiredService<PhysicalCache<object?>>().GetValue(new CacheKey(cacheName, key), cancellationToken);
        _ = await Assert.That((hit.RawValue as StoredText)?.Bytes).IsSameReferenceAs(bytes);
        _ = await Assert.That(hit.Value as string).IsEqualTo(replacement);
    }
}
