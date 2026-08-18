using System;
using System.Globalization;
using System.Text.Json;
using Google.Protobuf.WellKnownTypes;

namespace Squirix.Internal;

/// <summary>Maps JSON subtrees into protobuf well-known <see cref="Value" /> forms.</summary>
internal static class ProtoJsonCodec
{
    internal const string NumberEnvelopeDecimalKey = "\0squirix:decimal";
    internal const string NumberEnvelopeInt64Key = "\0squirix:int64";

    internal static Value CreateNumberEnvelope(string markerKey, string numberText)
    {
        var s = new Struct();
        s.Fields.Add(markerKey, Value.ForString(numberText));
        return Value.ForStruct(s);
    }

    internal static double NormalizeNumber(double value) => value;

    internal static Struct StructFromJson(JsonElement el)
    {
        var s = new Struct();
        foreach (var p in el.EnumerateObject())
            s.Fields[p.Name] = ValueFromJson(p.Value);

        return s;
    }

    internal static bool TryWriteNumberEnvelope(Utf8JsonWriter writer, Struct s)
    {
        if (s.Fields.Count != 1)
            return false;

        if (s.Fields.TryGetValue(NumberEnvelopeInt64Key, out var longField) && longField.KindCase is Value.KindOneofCase.StringValue && long.TryParse(
                longField.StringValue,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var longValue))
        {
            writer.WriteNumberValue(longValue);
            return true;
        }

        if (!s.Fields.TryGetValue(NumberEnvelopeDecimalKey, out var decimalField) || decimalField.KindCase != Value.KindOneofCase.StringValue || !decimal.TryParse(
                decimalField.StringValue,
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out var decimalValue))
            return false;
        writer.WriteNumberValue(decimalValue);
        return true;
    }

    internal static Value ValueFromJson(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => Value.ForStruct(StructFromJson(el)),
            JsonValueKind.Array => new Value { ListValue = ListFromJson(el) },
            JsonValueKind.String => Value.ForString(el.GetString()),
            JsonValueKind.Number => ConvertNumberToProtoValue(el),
            JsonValueKind.True => Value.ForBool(true),
            JsonValueKind.False => Value.ForBool(false),
            JsonValueKind.Null => Value.ForNull(),
            JsonValueKind.Undefined => Value.ForNull(),
            _ => throw new ArgumentOutOfRangeException(nameof(el), "Unsupported JSON value kind."),
        };
    }

    private static Value ConvertNumberToProtoValue(JsonElement element)
    {
        var asDouble = element.GetDouble();
        if (Math.Abs(asDouble) <= double.Epsilon && BitConverter.DoubleToInt64Bits(asDouble) != 0)
            return Value.ForNumber(asDouble);

        if (element.TryGetInt64(out var int64))
            return CreateNumberEnvelope(NumberEnvelopeInt64Key, int64.ToString(CultureInfo.InvariantCulture));

        if (element.TryGetDecimal(out var dec))
            return CreateNumberEnvelope(NumberEnvelopeDecimalKey, dec.ToString(CultureInfo.InvariantCulture));

        return Value.ForNumber(asDouble);
    }

    private static ListValue ListFromJson(JsonElement el)
    {
        var list = new ListValue();
        var values = list.Values;
        var length = el.GetArrayLength();
        for (var index = 0; index < length; index++)
            values.Add(ValueFromJson(el[index]));

        return list;
    }
}
