using System.Globalization;

namespace Squirix.Server.Node.Observability;

/// <summary>Formats Activity tag values as strings to avoid boxing value types into <see cref="System.Diagnostics.Activity.SetTag(string, object?)" />.</summary>
internal static class ActivityTagValues
{
    internal const string False = "false";

    internal const string True = "true";

    internal static string Bool(bool value) => value ? True : False;

    internal static string Double(double value) => value.ToString("G17", CultureInfo.InvariantCulture);

    internal static string Int32(int value) => value.ToString(CultureInfo.InvariantCulture);

    internal static string Int64(long value) => value.ToString(CultureInfo.InvariantCulture);
}
