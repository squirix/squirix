using Microsoft.CodeAnalysis;

namespace Squirix.Analyzers;

/// <summary>
/// Shared helpers for Squirix Roslyn analyzers.
/// </summary>
internal static class AnalyzerHelpers
{
    internal static bool IsCompilerOrGenerated(ISymbol symbol)
    {
        if (symbol.IsImplicitlyDeclared)
            return true;

        foreach (var attribute in symbol.GetAttributes())
        {
            var name = attribute.AttributeClass?.Name;
            if (name is "CompilerGeneratedAttribute" or "GeneratedCodeAttribute")
                return true;
        }

        return false;
    }

    internal static Location? GetBestLocation(ISymbol symbol)
    {
        foreach (var location in symbol.Locations)
        {
            if (location.IsInSource)
                return location;
        }

        return null;
    }
}
