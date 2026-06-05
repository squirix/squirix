using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;
using HttpEndpoints = Squirix.Server.Adapters.Rest.HttpEndpoints;

namespace Squirix.Server.UnitTests.Adapters.Rest;

/// <summary>
/// Characterization tests for REST payload size validation and bounded body materialization.
/// </summary>
public sealed class HttpEndpointsPayloadConversionTests : ServerUnitTestBase
{
    /// <summary>
    /// Payload size validation uses the provided serializer options instead of hard-coded defaults.
    /// </summary>
    [Fact]
    public void ValidateEntryHonorsProvidedSerializerOptions()
    {
        var entry = new CacheEntry<string> { Value = "option-sensitive", Version = 3 };
        var includeNullOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web) { DefaultIgnoreCondition = JsonIgnoreCondition.Never };
        var ignoreNullOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web) { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

        var includeNullSize = JsonSerializer.SerializeToUtf8Bytes(entry, includeNullOptions).Length;
        var ignoreNullSize = JsonSerializer.SerializeToUtf8Bytes(entry, ignoreNullOptions).Length;
        Assert.True(includeNullSize > ignoreNullSize);

        Assert.Null(HttpEndpoints.ValidateEntry(entry, includeNullOptions, includeNullSize));
        Assert.NotNull(HttpEndpoints.ValidateEntry(entry, includeNullOptions, ignoreNullSize));
        Assert.Null(HttpEndpoints.ValidateEntry(entry, ignoreNullOptions, ignoreNullSize));
    }

    /// <summary>
    /// Entry payload size limit uses UTF-8 byte length consistent with string serialization for the same options.
    /// </summary>
    [Fact]
    public void ValidateEntryMaxBytesMatchesUtf8ByteLengthOfSerializedJson()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var entry = new CacheEntry<string> { Value = "entry-size-check", Version = 99 };

        var asString = JsonSerializer.Serialize(entry, options);
        var asUtf8 = JsonSerializer.SerializeToUtf8Bytes(entry, options);

        Assert.Equal(Encoding.UTF8.GetByteCount(asString), asUtf8.Length);
        Assert.Null(HttpEndpoints.ValidateEntry(entry, options, asUtf8.Length));
        Assert.NotNull(HttpEndpoints.ValidateEntry(entry, options, asUtf8.Length - 1));
    }
}
