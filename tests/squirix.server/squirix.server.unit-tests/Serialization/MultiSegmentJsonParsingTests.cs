using System;
using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization;
using Squirix.Server.Storage.Journaling.Entries;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Serialization;

/// <summary>Tests JSON parsing with payloads split across multiple segments.</summary>
public sealed class MultiSegmentJsonParsingTests : UnitTestBase
{
    private static readonly JsonSerializerOptions StrictSerializerOptions = CreateStrictSerializerOptions();

    /// <summary>Cache entry DTO parsing handles segmented property names and values.</summary>
    [Fact]
    public void CacheEntryJsonParsesSegmentedPayload()
    {
        const string json = """{"v":{"$t":"s","v":"segmented-value"},"ver":1234567890123,"expirationTicks":50000000,"tags":{"region":"west"}}""";
        using var document = JsonDocument.Parse(CreateSequence(json));

        var parsed = DiscriminatedEntryJsonReader.TryElementToEntry<string>(document.RootElement, out var entry);

        Assert.True(parsed);
        Assert.NotNull(entry);
        Assert.Equal("segmented-value", entry.Value);
        Assert.Equal(1_234_567_890_123L, entry.Version);
        Assert.Equal(TimeSpan.FromSeconds(5), entry.Expiration);
        Assert.Equal("west", entry.Tags?["region"]);
    }

    /// <summary>Idempotency record parsing handles segmented property names, string values, and nested objects.</summary>
    [Fact]
    public void IdempotencyRecordParsesSegmentedPayload()
    {
        const string json =
            """{"operationId":"segmented-op","fingerprint":"fingerprint","createdUtc":"2026-05-01T02:03:04Z","outcome":{"kind":"insert"}}""";
        var reader = CreateReader(json);

        var record = JsonSerializer.Deserialize<PersistedIdempotencyRecord>(ref reader, StrictSerializerOptions);

        Assert.NotNull(record);
        Assert.Equal("segmented-op", record.OperationId);
        Assert.Equal("insert", record.Outcome.Kind);
    }

    private static JsonSerializerOptions CreateStrictSerializerOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            AllowDuplicateProperties = false,
            AllowTrailingCommas = false,
            PropertyNameCaseInsensitive = true,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        };
        options.Converters.Add(new JsonStringEnumConverter());
        options.TypeInfoResolverChain.Insert(0, SquirixJsonSerializerContext.Default);
        options.TypeInfoResolverChain.Add(new DefaultJsonTypeInfoResolver());
        return options;
    }

    private static Utf8JsonReader CreateReader(string json)
    {
        var sequence = CreateSequence(json);
        return new Utf8JsonReader(sequence);
    }

    private static ReadOnlySequence<byte> CreateSequence(string json)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        BufferSegment? first = null;
        BufferSegment? last = null;
        for (var i = 0; i < bytes.Length; i++)
        {
            var segment = new BufferSegment(bytes.AsMemory(i, 1));
            if (first is null)
            {
                first = segment;
                last = segment;
                continue;
            }

            last = last!.Append(segment);
        }

        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        public BufferSegment Append(BufferSegment segment)
        {
            segment.RunningIndex = RunningIndex + Memory.Length;
            Next = segment;
            return segment;
        }
    }
}
