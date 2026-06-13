using System.Reflection;

namespace Squirix.UnitTests.Architecture;

/// <summary>
/// Central access to the client SDK assembly for architecture rules.
/// </summary>
internal static class ClientArchitecture
{
    /// <summary>
    /// Gets the client SDK assembly under test.
    /// </summary>
    public static Assembly MainAssembly => typeof(SquirixClient).Assembly;
}
