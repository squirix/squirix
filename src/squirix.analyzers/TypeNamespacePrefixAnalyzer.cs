using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags types whose simple name starts with the immediate parent namespace segment (SQR007).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TypeNamespacePrefixAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR007";

    private static readonly LocalizableString Description =
        "Type simple names must not start with the immediate parent namespace segment " + "(drop the redundant leaf-folder prefix).";

    private static readonly LocalizableString MessageFormat = "Type name '{0}' starts with parent namespace segment '{1}'; remove the redundant prefix";
    private static readonly LocalizableString Title = "Avoid type names that repeat the parent namespace segment";
    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Naming", DiagnosticSeverity.Warning, true, Description);


    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeNamedType, SymbolKind.NamedType);
    }

    private static void AnalyzeNamedType(SymbolAnalysisContext context)
    {
        var type = (INamedTypeSymbol)context.Symbol;
        if (AnalyzerHelpers.IsCompilerOrGenerated(type))
            return;

        // Nested types still live under the outer type's namespace; the rule is namespace-based.
        if (!TryGetImmediateNamespaceSegment(type.ContainingNamespace, out var segment))
            return;

        var name = type.Name;
        if (!StartsWithNamespaceSegment(name, segment))
            return;

        var location = AnalyzerHelpers.GetBestLocation(type);
        if (location is null)
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, location, name, segment));
    }

    /// <summary>
    /// True when <paramref name="typeName" /> starts with <paramref name="segment" /> at a PascalCase boundary
    /// (exact match, or the next character is an uppercase letter).
    /// </summary>
    private static bool StartsWithNamespaceSegment(string typeName, string segment)
    {
        if (typeName.Length < segment.Length)
            return false;

        if (!typeName.StartsWith(segment, StringComparison.Ordinal))
            return false;

        if (typeName.Length == segment.Length)
            return true;

        return char.IsUpper(typeName[segment.Length]);
    }

    private static bool TryGetImmediateNamespaceSegment(INamespaceSymbol? ns, out string segment)
    {
        segment = string.Empty;
        if (ns is null || ns.IsGlobalNamespace)
            return false;

        segment = ns.Name;
        return segment.Length > 0;
    }
}
