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
            _ = escape is null ? sb.Append(ch) : sb.Append(escape);
        }

        _ = sb.Append('"');
    }

    private static string?[] BuildControlCharacterEscapes()
    {
        var escapes = new string?[128];
        escapes['"'] = "\\\"";
        escapes['\\'] = @"\\";
        escapes['\b'] = "\\b";
        escapes['\f'] = "\\f";
        escapes['\n'] = "\\n";
        escapes['\r'] = "\\r";
        escapes['\t'] = "\\t";
        for (var i = 0; i < 32; i++)
            escapes[i] ??= "\\u" + i.ToString("x4", CultureInfo.InvariantCulture);

        return escapes;
    }
}
