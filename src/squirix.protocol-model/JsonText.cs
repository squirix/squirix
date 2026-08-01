using System;
using System.Globalization;
using System.Text;

namespace Squirix.ProtocolModel;

internal static class JsonText
{
    internal static void AppendString(StringBuilder sb, string value)
    {
        _ = sb.Append('"');
        for (var i = 0; i < value.Length; i++)
        {
            var ch = value[i];
            switch (ch)
            {
                case '"':
                    _ = sb.Append("\\\"");
                    break;
                case '\\':
                    _ = sb.Append(@"\\");
                    break;
                case '\b':
                    _ = sb.Append("\\b");
                    break;
                case '\f':
                    _ = sb.Append("\\f");
                    break;
                case '\n':
                    _ = sb.Append("\\n");
                    break;
                case '\r':
                    _ = sb.Append("\\r");
                    break;
                case '\t':
                    _ = sb.Append("\\t");
                    break;
                default:
                    if (ch < ' ')
                    {
                        _ = sb.Append("\\u");
                        _ = sb.Append(Convert.ToUInt16(ch).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        _ = sb.Append(ch);
                    }

                    break;
            }
        }

        _ = sb.Append('"');
    }
}
