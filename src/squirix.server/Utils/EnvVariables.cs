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
    /// When the variable is unset or whitespace, returns <c>null</c>. When set, parses explicit <c>true</c>/<c>false</c>/<c>1</c>/<c>0</c> (case-insensitive).
    /// </summary>
    /// <param name="variableName">The environment variable name.</param>
    /// <returns>The parsed value, or <c>null</c> if unset.</returns>
    /// <exception cref="InvalidOperationException">The variable is set but its value is not a recognized boolean.</exception>
    internal static bool? ReadExplicitBool(string variableName)
    {
        var raw = ReadString(variableName);
        return string.IsNullOrWhiteSpace(raw) ? null : ParseExplicitBool(variableName, raw.Trim());
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

    /// <summary>
    /// Parses an explicit boolean for configuration ( <c>true</c>/<c>false</c>/<c>1</c>/<c>0</c>, case-insensitive).
    /// </summary>
    /// <param name="variableName">The environment variable name (for error messages).</param>
    /// <param name="trimmed">The trimmed raw value.</param>
    /// <returns>The parsed boolean.</returns>
    /// <exception cref="InvalidOperationException">The value is not recognized.</exception>
    private static bool ParseExplicitBool(string variableName, string trimmed)
    {
        if (trimmed.Equals("true", StringComparison.OrdinalIgnoreCase) || trimmed.Equals("1", StringComparison.OrdinalIgnoreCase))
            return true;

        if (trimmed.Equals("false", StringComparison.OrdinalIgnoreCase) || trimmed.Equals("0", StringComparison.OrdinalIgnoreCase))
            return false;

        throw new InvalidOperationException($"Invalid environment variable '{variableName}' value '{trimmed}'. Expected true, false, 1, or 0 (case-insensitive).");
    }
}
