namespace Squirix.Server.TestKit.Environment;

/// <summary>
/// Thin wrapper for reading process environment variables in tests (centralizes access alongside <see cref="TempEnvironmentVariable" />).
/// </summary>
public static class EnvVar
{
    /// <summary>Gets the value of an environment variable from the current process.</summary>
    /// <param name="variableName">The variable name.</param>
    /// <returns>The raw value, or <see langword="null"/> if the variable is not defined.</returns>
    public static string? Get(string variableName) => System.Environment.GetEnvironmentVariable(variableName);
}
