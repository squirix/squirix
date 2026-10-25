using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies that <see cref="CacheEntryCodec" /> Try paths return <see langword="false" /> for input they cannot map instead of throwing.</summary>
[Immutable]
public sealed class CacheEntryCodecRejectionTests : ServerUnitTestBase
{
    private const long AboveIntRange = int.MaxValue + 1L;
    private const long BelowIntRange = int.MinValue - 1L;
    private const long IntMaxAsLong = int.MaxValue;
    private const long IntMinAsLong = int.MinValue;
    private const int DocumentMaxDepth = 2048;

    /// <summary>A decoded array tree nested deeper than a JSON reader accepts by default fails to decode, however deep it is.</summary>
    /// <param name="depth">The nesting depth.</param>
    [Test]
    [Arguments(65)]
    [Arguments(1100)]
    public async Task DeepJsonArrayFailsToDecode(int depth)
    {
        var json = new string('[', depth) + new string(']', depth);

        _ = await Assert.That(DecodesJson(json)).IsFalse();
    }

    /// <summary>A decoded object tree nested deeper than a JSON reader accepts by default fails to decode.</summary>
    [Test]
    public async Task DeepJsonObjectFailsToDecode()
    {
        const int depth = 65;
        var json = new StringBuilder().Insert(0, "{\"a\":", depth).Append('1').Append('}', depth).ToString();

        _ = await Assert.That(DecodesJson(json)).IsFalse();
    }

    /// <summary>An integer outside the int range does not map into an int cache, and the long range still maps into a long cache.</summary>
    [Test]
    public async Task OutOfRangeIntegerDoesNotMapToInt()
    {
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(AboveIntRange), out _)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(BelowIntRange), out _)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(long.MaxValue), out _)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(IntMaxAsLong), out var max)).IsTrue();
        _ = await Assert.That(max!.Value).IsEqualTo(int.MaxValue);
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<int>(new NodeCacheEntry<object?>(IntMinAsLong), out var min)).IsTrue();
        _ = await Assert.That(min!.Value).IsEqualTo(int.MinValue);
        _ = await Assert.That(CacheEntryCodec.TryMapEntry<long>(new NodeCacheEntry<object?>(long.MaxValue), out _)).IsTrue();
    }

    private static bool DecodesJson(string json)
    {
        using var document = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = DocumentMaxDepth });
        var entry = new NodeCacheEntry<object?> { Value = document.RootElement.Clone(), Version = 1 };
        var readOk = new StrongBox<bool>();
        BufferKit.WithBuffer(
            CacheEntryCodec.ComputeEncodedLength(entry),
            (entry, readOk),
            static (ctx, buffer) =>
            {
                CacheEntryCodec.Write(ctx.entry, buffer);
                ctx.readOk.Value = CacheEntryCodec.TryRead<object?>(buffer, out _, out _);
            });

        return readOk.Value;
    }
}
