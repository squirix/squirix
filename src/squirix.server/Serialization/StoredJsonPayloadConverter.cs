using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Storage;

namespace Squirix.Server.Serialization;

/// <summary>
/// Serializes <see cref="StoredJsonPayload" /> by writing raw UTF-8 bytes directly.
/// Reads are supported by capturing the raw token bytes into a new payload.
/// </summary>
internal sealed class StoredJsonPayloadConverter : JsonConverter<StoredJsonPayload>
{
    public override StoredJsonPayload Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        return StoredJsonPayload.FromElement(doc.RootElement);
    }

    public override void Write(Utf8JsonWriter writer, StoredJsonPayload value, JsonSerializerOptions options) => value.WriteTo(writer);
}
