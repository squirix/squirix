using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A JSON value is one immutable byte sequence from the request to the response: stored without a copy and answered without one.</summary>
[Immutable]
public sealed class JsonValueOwnershipTests : IsolatedStorageTestBase
{
    private const string ObjectText = """{"__v":"shared","n":12345678901234567890}""";

    /// <summary>The stored value holds the bytes of the request, and a read answers with those same bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadAnswersTheBytesTheRequestCarried(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var request = FusedWriteHarness.Set(FusedWriteHarness.OpId(1));
        request.Entry.Value = JsonValueKit.Wire(ObjectText);
        _ = await harness.Adapter.SetEntry(request, new TestServerCallContext());

        var stored = await harness.Physical.GetValueAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken);
        var response = await harness.Adapter.GetValue(new GetValueAsyncRequest { CacheName = FusedWriteHarness.CacheName, Key = FusedWriteHarness.Key }, new TestServerCallContext());

        var value = (await Assert.That(stored.Value).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(value.Bytes.Span.Overlaps(request.Entry.Value.JsonUtf8.Memory.Span, out var storedOffset)).IsTrue();
        _ = await Assert.That(storedOffset).IsEqualTo(0);
        _ = await Assert.That(response.Value.JsonUtf8.Memory.Span.Overlaps(value.Bytes.Span, out var answeredOffset)).IsTrue();
        _ = await Assert.That(answeredOffset).IsEqualTo(0);
        _ = await Assert.That(response.Value.JsonUtf8.ToStringUtf8()).IsEqualTo(ObjectText);
    }

    /// <summary>An entry read answers the same bytes, and they equal the bytes that were stored.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntryReadAnswersTheStoredBytes(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var request = FusedWriteHarness.Set(FusedWriteHarness.OpId(1));
        request.Entry.Value = JsonValueKit.Wire(ObjectText);
        _ = await harness.Adapter.SetEntry(request, new TestServerCallContext());

        var response = await harness.Adapter.GetEntry(new GetEntryAsyncRequest { CacheName = FusedWriteHarness.CacheName, Key = FusedWriteHarness.Key }, new TestServerCallContext());

        _ = await Assert.That(response.Found).IsTrue();
        _ = await Assert.That(response.Entry.Value.JsonUtf8.ToStringUtf8()).IsEqualTo(ObjectText);
    }
}
