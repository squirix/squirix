using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire;

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
        request.Entry!.Value = JsonValueKit.Wire(ObjectText);
        _ = await harness.Adapter.Mutations.SetEntryAsync(request, CancellationToken.None);

        var stored = harness.Physical.GetValue(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken);
        var response = await harness.Adapter.Reads.GetValueAsync(new GetValueAsyncRequest { CacheName = FusedWriteHarness.CacheName, Key = FusedWriteHarness.Key }, CancellationToken.None);

        var value = (await Assert.That(stored.Value).IsTypeOf<JsonUtf8Value>())!;
        _ = await Assert.That(value.Bytes.Span.Overlaps(request.Entry.Value.JsonUtf8.Span, out var storedOffset)).IsTrue();
        _ = await Assert.That(storedOffset).IsEqualTo(0);
        _ = await Assert.That(response.Value!.JsonUtf8.Span.Overlaps(value.Bytes.Span, out var answeredOffset)).IsTrue();
        _ = await Assert.That(answeredOffset).IsEqualTo(0);
        _ = await Assert.That(Encoding.UTF8.GetString(response.Value.JsonUtf8.Span)).IsEqualTo(ObjectText);
    }

    /// <summary>An entry read answers the same bytes, and they equal the bytes that were stored.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntryReadAnswersTheStoredBytes(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var request = FusedWriteHarness.Set(FusedWriteHarness.OpId(1));
        request.Entry!.Value = JsonValueKit.Wire(ObjectText);
        _ = await harness.Adapter.Mutations.SetEntryAsync(request, CancellationToken.None);

        var response = await harness.Adapter.Reads.GetEntryAsync(new GetEntryAsyncRequest { CacheName = FusedWriteHarness.CacheName, Key = FusedWriteHarness.Key }, CancellationToken.None);

        _ = await Assert.That(response.Found).IsTrue();
        _ = await Assert.That(Encoding.UTF8.GetString(response.Entry!.Value!.JsonUtf8.Span)).IsEqualTo(ObjectText);
    }

    /// <summary>A get-or-add that adds answers without the value, and one over a present key answers the stored bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddAnswersValueOnlyIfStored(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var adding = FusedWriteHarness.GetOrAdd(FusedWriteHarness.OpId(1), "first");
        adding.Entry!.Value = JsonValueKit.Wire(ObjectText);
        var existing = FusedWriteHarness.GetOrAdd(FusedWriteHarness.OpId(2), "second");

        var added = await harness.Adapter.Mutations.GetOrAddAsync(adding, CancellationToken.None);
        var found = await harness.Adapter.Mutations.GetOrAddAsync(existing, CancellationToken.None);

        _ = await Assert.That(added.Added).IsTrue();
        _ = await Assert.That(added.Found).IsTrue();
        _ = await Assert.That(added.Value).IsNull();
        _ = await Assert.That(found.Added).IsFalse();
        _ = await Assert.That(found.Found).IsTrue();
        _ = await Assert.That(Encoding.UTF8.GetString(found.Value!.JsonUtf8.Span)).IsEqualTo(ObjectText);
    }
}
