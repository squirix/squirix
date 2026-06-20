using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix.Server.Storage;

/// <summary>Serializes <see cref="TimeSpan"/> as whole milliseconds in JSON configuration.</summary>
internal sealed class MillisecondsTimeSpanJsonConverter : JsonConverter<TimeSpan>
{
    public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType is JsonTokenType.Number && reader.TryGetInt64(out var milliseconds))
            return TimeSpan.FromMilliseconds(milliseconds);

        if (reader.TokenType is not JsonTokenType.String)
            throw new JsonException("Expected a millisecond count or TimeSpan string.");
        var text = reader.GetString();
        if (text is not null && TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var parsed))
            return parsed;

        throw new JsonException("Expected a millisecond count or TimeSpan string.");
    }

    public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options) =>
        writer.WriteNumberValue(Convert.ToInt64(value.TotalMilliseconds));
}
