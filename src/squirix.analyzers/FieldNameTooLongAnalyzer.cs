using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags fields with names that are too long (SQR006).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class FieldNameTooLongAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR006";

    private static readonly LocalizableString Description = "Field names must be at most 50 characters.";

    private static readonly LocalizableString MessageFormat = "Field name '{0}' length is {1} (limit {2})";
    private static readonly LocalizableString Title = "Avoid fields with name too long";
    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Naming", DiagnosticSeverity.Warning, true, Description);


    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeField, SymbolKind.Field);
    }

    private static void AnalyzeField(SymbolAnalysisContext context)
    {
        var field = (IFieldSymbol)context.Symbol;
        if (AnalyzerHelpers.IsCompilerOrGenerated(field))
            return;

        var name = field.Name;
        if (name.Length <= AnalyzerLimits.MaxFieldNameLength)
            return;

        var location = AnalyzerHelpers.GetBestLocation(field);
        if (location == null)
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, location, name, name.Length, AnalyzerLimits.MaxFieldNameLength));
    }
}
