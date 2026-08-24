using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags methods with names that are too long (SQR005).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class MethodNameTooLongAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR005";

    private static readonly LocalizableString Description =
        "Method simple names must be at most 40 characters " + "(excluding explicit interface implementations). Applies to production and test code.";

    private static readonly LocalizableString MessageFormat = "Method name '{0}' length is {1} (limit {2})";
    private static readonly LocalizableString Title = "Avoid methods with name too long";
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
        context.RegisterSymbolAction(AnalyzeMethod, SymbolKind.Method);
    }

    private static void AnalyzeMethod(SymbolAnalysisContext context)
    {
        var method = (IMethodSymbol)context.Symbol;
        if (!method.ExplicitInterfaceImplementations.IsDefaultOrEmpty)
            return;

        if (AnalyzerHelpers.IsCompilerOrGenerated(method))
            return;

        var name = method.Name;
        var effectiveLength = name.Length;

        // Property getter/setter are prefixed with "get_" / "set_" (length 4).
        if (method.MethodKind is MethodKind.PropertyGet or MethodKind.PropertySet)
            effectiveLength -= 4;

        if (effectiveLength <= AnalyzerLimits.MaxMethodNameLength)
            return;

        var location = AnalyzerHelpers.GetBestLocation(method);
        if (location == null)
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, location, name, effectiveLength, AnalyzerLimits.MaxMethodNameLength));
    }
}
