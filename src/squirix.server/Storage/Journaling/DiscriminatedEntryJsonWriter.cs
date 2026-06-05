using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using Squirix.Server.Serialization;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Builds discriminated entry JSON directly from CLR values.
/// Prefer using this on write-paths (REST/gRPC) to persist exact CLR types.
/// </summary>
internal static class DiscriminatedEntryJsonWriter
{
    public static byte[] BuildEntryJson(object? value, DateTime? expiresUtc, TimeSpan? expiration, long version, IReadOnlyDictionary<string, string>? tags)
    {
        using var buffer = new PooledByteBufferWriter();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();

            w.WritePropertyName("v");
            WriteClrWithDiscriminator(w, value);

            if (expiresUtc.HasValue)
                w.WriteString("expUtc", expiresUtc.Value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));

            if (expiration.HasValue)
                w.WriteNumber("expirationTicks", expiration.Value.Ticks);

            w.WriteNumber("ver", version);

            if (tags is not null)
            {
                w.WritePropertyName("tags");
                w.WriteStartObject();
                foreach (var kv in tags)
                    w.WriteString(kv.Key, kv.Value);
                w.WriteEndObject();
            }

            w.WriteEndObject();
        }

        return [.. buffer.WrittenSpan];
    }

    private static void WriteClrWithDiscriminator(Utf8JsonWriter w, object? value)
    {
        w.WriteStartObject();

        switch (value)
        {
            case null:
                w.WriteString("$t", "n");
                break;

            case bool b:
                w.WriteString("$t", "b");
                w.WritePropertyName("v");
                w.WriteBooleanValue(b);
                break;

            case string s:
                w.WriteString("$t", "s");
                w.WritePropertyName("v");
                w.WriteStringValue(s);
                break;

            case byte[] bytes:
                w.WriteString("$t", "ba");
                w.WritePropertyName("v");
                w.WriteStringValue(Convert.ToBase64String(bytes));
                break;

            case sbyte or byte or short or ushort or int or uint or long:
                w.WriteString("$t", "l");
                w.WritePropertyName("v");
                w.WriteNumberValue(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                break;

            case float or double:
                w.WriteString("$t", "d");
                w.WritePropertyName("v");
                w.WriteNumberValue(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                break;

            case decimal m:
                w.WriteString("$t", "m");
                w.WritePropertyName("v");
                w.WriteStringValue(m.ToString(CultureInfo.InvariantCulture));
                break;

            case StoredJsonPayload sjp:
                w.WriteString("$t", "j");
                w.WritePropertyName("v");
                sjp.WriteTo(w);
                break;

            case JsonElement je:
                w.WriteString("$t", "j");
                w.WritePropertyName("v");
                je.WriteTo(w);
                break;

            default:
                var elem = SerializationProvider.Instance.SerializeToElement(value);
                w.WriteString("$t", "j");
                w.WritePropertyName("v");
                elem.WriteTo(w);
                break;
        }

        w.WriteEndObject();
    }
}
