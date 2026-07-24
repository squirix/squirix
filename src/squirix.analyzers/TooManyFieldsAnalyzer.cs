using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags types with too many fields (SQR003).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TooManyFieldsAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR003";

    private static readonly LocalizableString Title = "Avoid types with too many fields";

    private static readonly LocalizableString MessageFormat =
        "Type '{0}' has {1} fields (limit {2}); prefer splitting state or introducing collaborators";

    private static readonly LocalizableString Description =
        "Types with more than 15 non-literal, non-static-readonly fields tend to hold too much state.";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId,
        Title,
        MessageFormat,
        "Design",
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
        if (type.TypeKind == TypeKind.Enum)
            return;

        if (AnalyzerHelpers.IsCompilerOrGenerated(type))
            return;

        var fieldCount = 0;
        foreach (var member in type.GetMembers())
        {
            if (member is not IFieldSymbol field)
                continue;

            if (!ShouldCountField(field))
                continue;

            fieldCount++;
        }

        if (fieldCount <= AnalyzerLimits.MaxFieldsPerType)
            return;

        var location = AnalyzerHelpers.GetBestLocation(type);
        if (location is null)
            return;

        context.ReportDiagnostic(
            Diagnostic.Create(
                Rule,
                location,
                type.Name,
                fieldCount,
                AnalyzerLimits.MaxFieldsPerType));
    }

    private static bool ShouldCountField(IFieldSymbol field)
    {
        if (AnalyzerHelpers.IsCompilerOrGenerated(field))
            return false;

        if (field.IsConst)
            return false;

        if (field is { IsStatic: true, IsReadOnly: true })
            return false;

        return true;
    }
}
