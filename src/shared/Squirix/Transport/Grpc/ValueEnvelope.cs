using System;
using System.Globalization;
using System.Text.Json;
using Google.Protobuf.WellKnownTypes;

namespace Squirix.Transport.Grpc;

/// <summary>
/// Single source of truth for the scalar/number envelope wire format shared by the client SDK and the server runtime.
/// The marker keys are part of the on-the-wire contract and must never diverge between assemblies.
/// </summary>
internal static class ValueEnvelope
{
    /// <summary>Struct field name that wraps a lone scalar payload.</summary>
    internal const string ScalarEnvelopeKey = "\0squirix:scalar";

    /// <summary>Marker key wrapping an int64 payload as an invariant string.</summary>
    internal const string NumberEnvelopeInt64Key = "\0squirix:int64";

    /// <summary>Marker key wrapping a decimal payload as an invariant string.</summary>
    internal const string NumberEnvelopeDecimalKey = "\0squirix:decimal";

    /// <summary>Wraps a single value under one struct field.</summary>
    /// <param name="fieldName">Name of the sole struct field.</param>
    /// <param name="value">Payload value to wrap.</param>
    /// <returns>A struct containing exactly one field named <paramref name="fieldName" />.</returns>
    internal static Struct WrapAsStruct(string fieldName, Value value) => new() { Fields = { [fieldName] = value } };

    /// <summary>Builds a single-field struct envelope carrying a number as an invariant string.</summary>
    /// <param name="markerKey">Marker key selecting the wrapped number kind.</param>
    /// <param name="numberText">Invariant-culture number text payload.</param>
    /// <returns>The envelope value ready to be stored inside a struct.</returns>
    internal static Value CreateNumberEnvelope(string markerKey, string numberText)
    {
        var s = new Struct();
        s.Fields.Add(markerKey, Value.ForString(numberText));
        return Value.ForStruct(s);
    }

    /// <summary>Converts a JSON number into a proto value, preserving int64/decimal precision via envelopes.</summary>
    /// <param name="element">JSON number element to convert.</param>
    /// <returns>The proto value matching the numeric precision of the element.</returns>
    internal static Value ConvertJsonNumberToProtoValue(JsonElement element)
    {
        var asDouble = element.GetDouble();
        if (Math.Abs(asDouble) <= double.Epsilon && BitConverter.DoubleToInt64Bits(asDouble) != 0)
            return Value.ForNumber(asDouble);

        if (element.TryGetInt64(out var asInt64))
            return CreateNumberEnvelope(NumberEnvelopeInt64Key, asInt64.ToString(CultureInfo.InvariantCulture));

        if (element.TryGetDecimal(out var asDecimal))
            return CreateNumberEnvelope(NumberEnvelopeDecimalKey, asDecimal.ToString(CultureInfo.InvariantCulture));

        return Value.ForNumber(asDouble);
    }

    /// <summary>Writes a single-field number envelope back as a JSON number when the struct matches the marker format.</summary>
    /// <param name="writer">JSON writer receiving the number.</param>
    /// <param name="structValue">Struct to inspect for a number envelope.</param>
    /// <returns><see langword="true" /> when the envelope was recognized and written.</returns>
    internal static bool TryWriteNumberEnvelope(Utf8JsonWriter writer, Struct structValue)
    {
        if (structValue.Fields.Count != 1)
            return false;

        if (structValue.Fields.TryGetValue(NumberEnvelopeInt64Key, out var longField) && longField.KindCase is Value.KindOneofCase.StringValue && long.TryParse(
                longField.StringValue,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var longValue))
        {
            writer.WriteNumberValue(longValue);
            return true;
        }

        if (!structValue.Fields.TryGetValue(NumberEnvelopeDecimalKey, out var decimalField) || decimalField.KindCase != Value.KindOneofCase.StringValue || !decimal.TryParse(
                decimalField.StringValue,
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out var decimalValue))
            return false;
        writer.WriteNumberValue(decimalValue);
        return true;
    }
}
