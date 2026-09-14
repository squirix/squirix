using System.Globalization;
using System.Text;

namespace Squirix.ProtocolModel;

internal static class JsonText
{
    private static readonly string?[] ControlCharacterEscapes = BuildControlCharacterEscapes();

    internal static void AppendString(StringBuilder sb, string value)
    {
        _ = sb.Append('"');
        for (var i = 0; i < value.Length; i++)
        {
            var ch = value[i];
            var escape = ch < ControlCharacterEscapes.Length ? ControlCharacterEscapes[ch] : null;
            _ = escape == null ? sb.Append(ch) : sb.Append(escape);
        }

        _ = sb.Append('"');
    }

    private static string?[] BuildControlCharacterEscapes()
    {
        var escapes = new string?[128];
        escapes[34] = "\\\"";
        escapes[92] = @"\\";
        escapes[8] = "\\b";
        escapes[12] = "\\f";
        escapes[10] = "\\n";
        escapes[13] = "\\r";
        escapes[9] = "\\t";
        for (var i = 0; i < 32; i++)
            escapes[i] ??= "\\u" + i.ToString("x4", CultureInfo.InvariantCulture);

        return escapes;
    }
}
