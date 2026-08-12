using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Core;

/// <summary>
/// <see cref="IServerSerializer" /> implementation backed by <see cref="System.Text.Json" />.
/// </summary>
/// <remarks>
/// Intentional reflection fallback for arbitrary application payload types.
/// Persistence and health/metrics HTTP DTOs use dedicated <see cref="JsonSerializerContext" /> types at call sites.
/// </remarks>
#pragma warning disable ZA1001 // Generic serializer boundary; reflection fallback is required for unknown T.
internal sealed class ServerJsonSerializer : IServerSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServerJsonSerializer" /> class.
    /// </summary>
    internal ServerJsonSerializer()
    {
        _options = CreateDefaultOptions();
    }

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
