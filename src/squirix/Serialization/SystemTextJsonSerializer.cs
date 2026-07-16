using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Serialization;

/// <summary>
/// <see cref="ISquirixSerializer" /> implementation backed by <see cref="System.Text.Json" />.
/// </summary>
/// <remarks>
/// Intentional reflection fallback for arbitrary user cache value types (<c>Deserialize&lt;T&gt;</c> / <c>Serialize&lt;T&gt;</c>).
/// Known squirix DTOs should use dedicated <see cref="JsonSerializerContext" /> types at call sites instead of this class.
/// </remarks>
#pragma warning disable ZA1001 // ISquirixSerializer must support arbitrary T; reflection fallback is the public contract.
internal sealed class SystemTextJsonSerializer : ISquirixSerializer
{
    private readonly JsonSerializerOptions _options = CreateDefaultOptions();

    /// <inheritdoc />
    public T? Deserialize<T>(string payload) => JsonSerializer.Deserialize<T>(payload, _options);

    /// <inheritdoc />
    public T? Deserialize<T>(JsonElement payload) => payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null ? default : payload.Deserialize<T>(_options);

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlySpan<byte> payload) => JsonSerializer.Deserialize<T>(payload, _options);

    /// <inheritdoc />
    public T? Deserialize<T>(Stream payload) => JsonSerializer.Deserialize<T>(payload, _options);

    /// <inheritdoc />
    public void Serialize<T>(Stream destination, T? value) => JsonSerializer.Serialize(destination, value, _options);

    /// <inheritdoc />
    public JsonElement SerializeToElement<T>(T? value) => JsonSerializer.SerializeToElement(value, _options);

    /// <inheritdoc />
    public byte[] SerializeToUtf8Bytes<T>(T? value) => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    private static JsonSerializerOptions CreateDefaultOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            PropertyNameCaseInsensitive = true,
            ReadCommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        options.Converters.Add(new JsonStringEnumConverter());
        options.TypeInfoResolverChain.Add(new DefaultJsonTypeInfoResolver());
        return options;
    }
}
#pragma warning restore ZA1001
