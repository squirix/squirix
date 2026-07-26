using Squirix.Server.Utils;

namespace Squirix.Server.Node.Observability;

/// <summary>Formats Activity tag values as strings to avoid boxing value types into <see cref="System.Diagnostics.Activity.SetTag(string, object?)" />.</summary>
internal static class ActivityTagValues
{
    internal const string False = "false";

    internal const string True = "true";

    internal static string Bool(bool value) => value ? True : False;

    internal static string Double(double value) => InvariantDigitStrings.Format(value);

    internal static string Int32(int value) => InvariantDigitStrings.Format(value);

    internal static string Int64(long value) => InvariantDigitStrings.Format(value);

    internal static string UInt64(ulong value) => InvariantDigitStrings.Format(value);
}
