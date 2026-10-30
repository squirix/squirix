using System.Text.RegularExpressions;

namespace Squirix.Server.UnitTests.SourceScanning;

/// <summary>Token-oriented replication guards applied after masking comments and literals.</summary>
internal static partial class ReplicationSourceRules
{
    internal static Regex ServiceLocator => ServiceLocatorPattern;

    internal static Regex DebugOutput => DebugOutputPattern;

    [GeneratedRegex(
        @"\b(?:IServiceProvider|IServiceScope|IServiceScopeFactory|ServiceProvider)\b|\b(?:GetService|GetRequiredService|GetServices|CreateScope|CreateAsyncScope)\s*(?:<|\()",
        RegexOptions.CultureInvariant, 1000)]
    private static partial Regex ServiceLocatorPattern { get; }

    [GeneratedRegex(
        @"\bNewtonsoft\b|\bSystem\s*\.\s*Dynamic\b|\bConsole\s*\.\s*(?:Write|WriteLine|Error|Out)\b|\bDump\s*(?:<|\()",
        RegexOptions.CultureInvariant, 1000)]
    private static partial Regex DebugOutputPattern { get; }

    internal static bool HasViolation(Regex pattern, string source) => pattern.IsMatch(CsharpSourceMasker.Mask(source));
}
