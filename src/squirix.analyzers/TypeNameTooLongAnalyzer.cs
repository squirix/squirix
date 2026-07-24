using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags types with names that are too long (SQR004).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TypeNameTooLongAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR004";

    private static readonly LocalizableString Title = "Avoid types with name too long";

    private static readonly LocalizableString MessageFormat =
        "Type name '{0}' length is {1} (limit {2})";

    private static readonly LocalizableString Description =
        "Type simple names must be at most 40 characters.";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId,
        Title,
        MessageFormat,
        "Naming",
        DiagnosticSeverity.Warning,
        true,
        description: Description);

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
            throw new System.ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeNamedType, SymbolKind.NamedType);
    }

    private static void AnalyzeNamedType(SymbolAnalysisContext context)
    {
        var type = (INamedTypeSymbol)context.Symbol;
        if (AnalyzerHelpers.IsCompilerOrGenerated(type))
            return;

        var name = type.Name;
        if (name.Length <= AnalyzerLimits.MaxTypeNameLength)
            return;

        var location = AnalyzerHelpers.GetBestLocation(type);
        if (location is null)
            return;

        context.ReportDiagnostic(
            Diagnostic.Create(
                Rule,
                location,
                name,
                name.Length,
                AnalyzerLimits.MaxTypeNameLength));
    }
}
