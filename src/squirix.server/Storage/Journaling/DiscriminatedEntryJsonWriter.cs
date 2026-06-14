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

    private static void WriteBoolDiscriminant(Utf8JsonWriter w, bool value)
    {
        w.WriteString("$t", "b");
        w.WritePropertyName("v");
        w.WriteBooleanValue(value);
    }

    private static void WriteBytesDiscriminant(Utf8JsonWriter w, byte[] value)
    {
        w.WriteString("$t", "ba");
        w.WritePropertyName("v");
        w.WriteStringValue(Convert.ToBase64String(value));
    }

    private static void WriteClrWithDiscriminator(Utf8JsonWriter w, object? value)
    {
        w.WriteStartObject();

        switch (value)
        {
            case null:
                WriteNullDiscriminant(w);
                break;

            case bool b:
                WriteBoolDiscriminant(w, b);
                break;

            case string s:
                WriteStringDiscriminant(w, s);
                break;

            case byte[] bytes:
                WriteBytesDiscriminant(w, bytes);
                break;

            case sbyte or byte or short or ushort or int or uint or long:
                WriteInt64Discriminant(w, value);
                break;

            case float or double:
                WriteDoubleDiscriminant(w, value);
                break;

            case decimal m:
                WriteDecimalDiscriminant(w, m);
                break;

            case StoredJsonPayload sjp:
                WriteJsonPayloadDiscriminant(w, sjp);
                break;

            case JsonElement je:
                WriteJsonElementDiscriminant(w, je);
                break;

            default:
                WriteSerializedDiscriminant(w, value);
                break;
        }

        w.WriteEndObject();
    }

    private static void WriteDecimalDiscriminant(Utf8JsonWriter w, decimal value)
    {
        w.WriteString("$t", "m");
        w.WritePropertyName("v");
        w.WriteStringValue(value.ToString(CultureInfo.InvariantCulture));
    }

    private static void WriteDoubleDiscriminant(Utf8JsonWriter w, object value)
    {
        w.WriteString("$t", "d");
        w.WritePropertyName("v");
        w.WriteNumberValue(Convert.ToDouble(value, CultureInfo.InvariantCulture));
    }

    private static void WriteInt64Discriminant(Utf8JsonWriter w, object value)
    {
        w.WriteString("$t", "l");
        w.WritePropertyName("v");
        w.WriteNumberValue(Convert.ToInt64(value, CultureInfo.InvariantCulture));
    }

    private static void WriteJsonElementDiscriminant(Utf8JsonWriter w, JsonElement value)
    {
        w.WriteString("$t", "j");
        w.WritePropertyName("v");
        value.WriteTo(w);
    }

    private static void WriteJsonPayloadDiscriminant(Utf8JsonWriter w, StoredJsonPayload value)
    {
        w.WriteString("$t", "j");
        w.WritePropertyName("v");
        value.WriteTo(w);
    }

    private static void WriteNullDiscriminant(Utf8JsonWriter w) => w.WriteString("$t", "n");

    private static void WriteSerializedDiscriminant(Utf8JsonWriter w, object value)
    {
        var elem = SerializationProvider.Instance.SerializeToElement(value);
        w.WriteString("$t", "j");
        w.WritePropertyName("v");
        elem.WriteTo(w);
    }

    private static void WriteStringDiscriminant(Utf8JsonWriter w, string value)
    {
        w.WriteString("$t", "s");
        w.WritePropertyName("v");
        w.WriteStringValue(value);
    }
}
