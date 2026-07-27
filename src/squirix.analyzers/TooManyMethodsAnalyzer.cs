using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags types with too many methods (SQR002).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TooManyMethodsAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR002";

    private static readonly LocalizableString Description = "Types with more than 20 instance/static methods " +
                                                            "(excluding constructors, property/event accessors) tend to have too many responsibilities. " +
                                                            "Stateless types with only constants are not matched.";

    private static readonly LocalizableString MessageFormat = "Type '{0}' has {1} methods (limit {2}); prefer splitting responsibilities";
    private static readonly LocalizableString Title = "Avoid types with too many methods";
    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Design", DiagnosticSeverity.Warning, true, Description);


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
        if (type.TypeKind == TypeKind.Interface)
            return;

        if (AnalyzerHelpers.IsCompilerOrGenerated(type))
            return;

        var hasNonLiteralField = false;
        var methodCount = 0;

        foreach (var member in type.GetMembers())
        {
            if (member is IFieldSymbol field)
            {
                if (!field.IsConst && !AnalyzerHelpers.IsCompilerOrGenerated(field))
                    hasNonLiteralField = true;

                continue;
            }

            if (member is not IMethodSymbol method)
                continue;

            if (!ShouldCountMethod(method))
                continue;

            methodCount++;
        }

        // Require at least one non-constant field (stateless utility types are allowed).
        if (!hasNonLiteralField)
            return;

        if (methodCount <= AnalyzerLimits.MaxMethodsPerType)
            return;

        var location = AnalyzerHelpers.GetBestLocation(type);
        if (location is null)
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, location, type.Name, methodCount, AnalyzerLimits.MaxMethodsPerType));
    }

    private static bool ShouldCountMethod(IMethodSymbol method)
    {
        if (AnalyzerHelpers.IsCompilerOrGenerated(method))
            return false;

        return method.MethodKind switch
        {
            MethodKind.Constructor or MethodKind.StaticConstructor => false,
            MethodKind.PropertyGet or MethodKind.PropertySet => false,
            MethodKind.EventAdd or MethodKind.EventRemove or MethodKind.EventRaise => false,
            MethodKind.Ordinary or MethodKind.UserDefinedOperator or MethodKind.Conversion => true,
            _ => false,
        };
    }
}
