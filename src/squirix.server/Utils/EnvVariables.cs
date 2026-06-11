using System;
using System.Globalization;

namespace Squirix.Server.Utils;

/// <summary>
/// Reads process environment variables with consistent parsing for Squirix configuration.
/// </summary>
internal static class EnvVariables
{
    /// <summary>
    /// Interprets common truthy environment values ( <c>true</c> or <c>1</c>, case-insensitive) as <see langword="true" />.
    /// </summary>
    /// <param name="variableName">The environment variable name.</param>
    /// <returns>Whether the variable is set to a truthy value.</returns>
    internal static bool ReadBool(string variableName)
    {
        var rawValue = ReadString(variableName);
        return rawValue is not null && (rawValue.Equals("true", StringComparison.OrdinalIgnoreCase) || rawValue.Equals("1", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Reads a signed 32-bit integer from the environment.
    /// </summary>
    /// <param name="variableName">The environment variable name.</param>
    /// <returns>The parsed value, or <c>null</c> when the variable is unset or whitespace.</returns>
    /// <exception cref="InvalidOperationException">The variable is set but its value is not a valid integer.</exception>
    internal static int? ReadInt(string variableName)
    {
        var raw = ReadString(variableName);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var trimmed = raw.Trim();
        return int.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new InvalidOperationException($"Invalid environment variable '{variableName}' value '{raw}'. Expected a valid integer.");
    }

    /// <summary>
    /// Reads a signed 64-bit integer from the environment.
    /// </summary>
    /// <param name="variableName">The environment variable name.</param>
    /// <returns>The parsed value, or <c>null</c> when the variable is unset or whitespace.</returns>
    /// <exception cref="InvalidOperationException">The variable is set but its value is not a valid integer.</exception>
    internal static long? ReadInt64(string variableName)
    {
        var raw = ReadString(variableName);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var trimmed = raw.Trim();
        return long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new InvalidOperationException($"Invalid environment variable '{variableName}' value '{raw}'. Expected a valid integer.");
    }

    /// <summary>
    /// Returns the raw environment variable value, or <c>null</c> when unset.
    /// </summary>
    /// <param name="variableName">The environment variable name.</param>
    /// <returns>The raw value, or <c>null</c> if the variable is not defined.</returns>
    internal static string? ReadString(string variableName) => Environment.GetEnvironmentVariable(variableName);

    /// <summary>
    /// Returns the environment variable value, or an empty string when unset.
    /// </summary>
    /// <param name="variableName">The environment variable name.</param>
    /// <returns>The raw value, or <see cref="string.Empty" /> if the variable is not defined.</returns>
    internal static string ReadStringOrEmpty(string variableName) => ReadString(variableName) ?? string.Empty;
}
