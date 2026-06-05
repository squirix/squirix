using System.Reflection;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Central access to the server assembly for architecture rules.
/// </summary>
public static class SquirixArchitecture
{
    /// <summary>
    /// Gets the server runtime assembly under test.
    /// </summary>
    public static Assembly ServerAssembly => typeof(SquirixServer).Assembly;
}
