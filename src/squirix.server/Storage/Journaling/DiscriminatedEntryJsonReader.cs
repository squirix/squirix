using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Squirix.Server.Storage.Journaling;

internal static class DiscriminatedEntryJsonReader
{
    private const string FieldExpirationTicks = "expirationTicks";
    private const string FieldExpiresUtc = "expUtc";
    private const string FieldTags = "tags";
    private const string FieldTypeTag = "$t";
    private const string FieldValue = "v";
    private const string FieldVersion = "ver";
    private const string TagBool = "b";
    private const string TagBytes = "ba";
    private const string TagDecimal = "m";
    private const string TagDouble = "d";

    private const string TagInt32 = "i";
    private const string TagInt64 = "l";
    private const string TagJson = "j";
    private const string TagNull = "n";
    private const string TagString = "s";

    public static bool TryElementToEntry<T>(JsonElement root, out CacheEntry<T> entry)
    {
        entry = null!;
        if (root.ValueKind != JsonValueKind.Object)
            return false;

        if (!root.TryGetProperty(FieldValue, out var valueEl) || valueEl.ValueKind != JsonValueKind.Object)
            return false;

        if (!valueEl.TryGetProperty(FieldTypeTag, out var tagEl) || tagEl.ValueKind != JsonValueKind.String)
            return false;

        var tag = tagEl.GetString();
        var hasInner = valueEl.TryGetProperty(FieldValue, out var inner);

        if (!TryReadDiscriminated(tag, hasInner, inner, out var clrValue))
            return false;

        if (!TryCoerceTo<T>(clrValue, out var typedValue))
            return false;

        DateTime? expUtc = null;
        if (root.TryGetProperty(FieldExpiresUtc, out var expEl) && expEl.ValueKind == JsonValueKind.String && DateTime.TryParse(
                expEl.GetString(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                out var dt))
        {
            expUtc = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        }

        long version = 1;
        if (root.TryGetProperty(FieldVersion, out var verEl) && verEl.ValueKind == JsonValueKind.Number && verEl.TryGetInt64(out var v) && v > 0)
            version = v;

        TimeSpan? expiration = null;
        if (root.TryGetProperty(FieldExpirationTicks, out var expirationTicksEl) && expirationTicksEl.ValueKind == JsonValueKind.Number &&
            expirationTicksEl.TryGetInt64(out var expirationTicks))
        {
            expiration = TimeSpan.FromTicks(expirationTicks);
        }

        FrozenDictionary<string, string>? tags = null;
        if (root.TryGetProperty(FieldTags, out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Object)
        {
            var dict = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var p in tagsEl.EnumerateObject())
                dict[p.Name] = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() ?? string.Empty : p.Value.GetRawText();
            tags = dict.ToFrozenDictionary(StringComparer.Ordinal);
        }

        entry = new CacheEntry<T>
        {
            Value = typedValue,
            ExpiresUtc = expUtc,
            Expiration = expiration,
            Version = version,
            Tags = tags,
        };
        return true;
    }

    public static bool TryUtf8ToEntry<T>(ReadOnlyMemory<byte> utf8Json, out CacheEntry<T> entry)
    {
        using var doc = JsonDocument.Parse(utf8Json);
        return TryElementToEntry(doc.RootElement, out entry);
    }

    private static TTarget Reinterpret<TTarget, TValue>(TValue value)
        where TValue : struct => Unsafe.As<TValue, TTarget>(ref value);

    private static bool TryCoerceTo<T>(object? value, out T? result)
    {
        switch (value)
        {
            case null:
                result = default;
                return true;

            case T ok:
                result = ok;
                return true;

            case StoredJsonPayload sjp when typeof(T) == typeof(JsonElement):
            {
                using var doc = JsonDocument.Parse(sjp.Utf8Memory);
                var element = doc.RootElement.Clone();
                result = Reinterpret<T, JsonElement>(element);
                return true;
            }

            case JsonElement je when typeof(T) == typeof(JsonElement):
                result = Reinterpret<T, JsonElement>(je);
                return true;

            default:
                result = default;
                return false;
        }
    }

    private static bool TryReadBoolDiscriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner)
            return false;

        if (inner.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
            return false;

        result = inner.GetBoolean();
        return true;
    }

    private static bool TryReadBytesDiscriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner || inner.ValueKind != JsonValueKind.String)
            return false;

        try
        {
            result = Convert.FromBase64String(inner.GetString() ?? string.Empty);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool TryReadDecimalDiscriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner || inner.ValueKind != JsonValueKind.String)
            return false;

        if (!decimal.TryParse(inner.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var m))
            return false;

        result = m;
        return true;
    }

    private static bool TryReadDiscriminated(string? tag, bool hasInner, JsonElement inner, out object? result)
    {
        result = null;

        return tag switch
        {
            TagNull => true,
            TagBool => TryReadBoolDiscriminant(hasInner, inner, out result),
            TagString => TryReadStringDiscriminant(hasInner, inner, out result),
            TagBytes => TryReadBytesDiscriminant(hasInner, inner, out result),
            TagInt32 => TryReadInt32Discriminant(hasInner, inner, out result),
            TagInt64 => TryReadInt64Discriminant(hasInner, inner, out result),
            TagDecimal => TryReadDecimalDiscriminant(hasInner, inner, out result),
            TagDouble => TryReadDoubleDiscriminant(hasInner, inner, out result),
            TagJson => TryReadJsonDiscriminant(hasInner, inner, out result),
            _ => false,
        };
    }

    private static bool TryReadDoubleDiscriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner || inner.ValueKind != JsonValueKind.Number)
            return false;

        if (!inner.TryGetDouble(out var d))
            return false;

        result = d;
        return true;
    }

    private static bool TryReadInt32Discriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner || inner.ValueKind != JsonValueKind.Number)
            return false;

        if (!inner.TryGetInt32(out var i))
            return false;

        result = i;
        return true;
    }

    private static bool TryReadInt64Discriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner || inner.ValueKind != JsonValueKind.Number)
            return false;

        if (!inner.TryGetInt64(out var l))
            return false;

        result = l;
        return true;
    }

    private static bool TryReadJsonDiscriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner)
            return false;

        result = StoredJsonPayload.FromElement(inner);
        return true;
    }

    private static bool TryReadStringDiscriminant(bool hasInner, JsonElement inner, out object? result)
    {
        result = null;
        if (!hasInner)
            return false;

        result = inner.ValueKind == JsonValueKind.String ? inner.GetString() : inner.ToString();
        return true;
    }
}
