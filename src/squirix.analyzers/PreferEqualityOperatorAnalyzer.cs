using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Prefers the '==' / '!=' operators over 'is' / 'is not' patterns for null checks and
/// constant-value comparisons (SQR012-SQR014). Reverses the Meziantou rules MA0142,
/// MA0148, and MA0149.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PreferEqualityOperatorAnalyzer : DiagnosticAnalyzer
{
    private const string Category = "Style";
    private const string IsConstantRuleId = "SQR013";
    private const string IsNotConstantRuleId = "SQR014";
    private const string NullCheckRuleId = "SQR012";

    private static readonly LocalizableString IsConstantDescription = "Constant-value comparison reads more clearly with the '==' operator.";

    private static readonly LocalizableString IsConstantMessage = "Use '==' instead of 'is' to compare against a constant value";

    private static readonly LocalizableString IsConstantTitle = "Prefer '==' over 'is' for constant values";
    private static readonly LocalizableString IsNotConstantDescription = "Constant-value inequality reads more clearly with the '!=' operator.";
    private static readonly LocalizableString IsNotConstantMessage = "Use '!=' instead of 'is not' to compare against a constant value";
    private static readonly LocalizableString IsNotConstantTitle = "Prefer '!=' over 'is not' for constant values";

    private static readonly LocalizableString NullCheckDescription =
        "Null checks read more clearly with the equality operators; prefer 'x == null' and 'x != null' over 'x is null' and 'x is not null'.";

    private static readonly LocalizableString NullCheckMessage = "Use '{0}' instead of '{1}' for the null check";
    private static readonly LocalizableString NullCheckTitle = "Prefer '==' / '!=' over 'is' / 'is not' for null checks";

    private static readonly DiagnosticDescriptor NullCheckRule = new(NullCheckRuleId, NullCheckTitle, NullCheckMessage, Category, DiagnosticSeverity.Warning, true,
        NullCheckDescription);

    private static readonly DiagnosticDescriptor IsConstantRule = new(IsConstantRuleId, IsConstantTitle, IsConstantMessage, Category, DiagnosticSeverity.Warning, true,
        IsConstantDescription);


    private static readonly DiagnosticDescriptor IsNotConstantRule = new(IsNotConstantRuleId, IsNotConstantTitle, IsNotConstantMessage, Category, DiagnosticSeverity.Warning, true,
        IsNotConstantDescription);


    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [NullCheckRule, IsConstantRule, IsNotConstantRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeIsPattern, SyntaxKind.IsPatternExpression);
    }

    private static void AnalyzeIsPattern(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not IsPatternExpressionSyntax isPattern)
            return;

        switch (isPattern.Pattern)
        {
            case ConstantPatternSyntax constantPattern:
                ReportPattern(context, isPattern, constantPattern, false);
                break;
            case UnaryPatternSyntax unaryPattern when unaryPattern.IsKind(SyntaxKind.NotPattern):
                ReportPattern(context, isPattern, unaryPattern.Pattern, true);
                break;
        }
    }

    private static bool CanUseEqualityOperator(SyntaxNodeAnalysisContext context, ExpressionSyntax expression)
    {
        var type = context.SemanticModel.GetTypeInfo(expression, context.CancellationToken).Type;
        if (type == null)
            return false;

        return type.SpecialType != SpecialType.System_Object && type.TypeKind is not (TypeKind.Interface or TypeKind.TypeParameter or TypeKind.Dynamic);
    }

    private static bool HasEqualityOperatorMembers(ITypeSymbol type)
    {
        if (type is not INamedTypeSymbol named)
            return false;

        foreach (var member in named.GetMembers())
        {
            if (member is IMethodSymbol { MethodKind: MethodKind.UserDefinedOperator, IsStatic: true, Name: "op_Equality" or "op_Inequality" })
                return true;
        }

        return false;
    }

    private static bool HasUserDefinedEqualityOperator(ITypeSymbol type)
    {
        var current = type;
        while (current != null && current.SpecialType != SpecialType.System_Object && current.TypeKind != TypeKind.Interface)
        {
            // System.String declares '==' / '!=' in the runtime, but their null behavior is
            // equivalent to the built-in null pattern, so they never block SQR012.
            if (current.SpecialType == SpecialType.System_String)
                return false;

            if (HasEqualityOperatorMembers(current))
                return true;

            current = current.BaseType;
        }

        return false;
    }

    private static bool IsNotANumber(object? value) => value is float.NaN or double.NaN;

    private static bool IsNullLiteral(ExpressionSyntax expression) => expression.IsKind(SyntaxKind.NullLiteralExpression);

    private static void ReportPattern(SyntaxNodeAnalysisContext context, IsPatternExpressionSyntax isPattern, PatternSyntax pattern, bool negated)
    {
        if (pattern is not ConstantPatternSyntax constantPattern)
            return;

        if (IsNullLiteral(constantPattern.Expression))
        {
            if (!CanUseEqualityOperator(context, isPattern.Expression))
                return;

            if (!ResolvesToBuiltInNullOperator(context, isPattern.Expression))
                return;

            context.ReportDiagnostic(Diagnostic.Create(NullCheckRule, pattern.GetLocation(), negated ? "!=" : "==", negated ? "is not null" : "is null"));
            return;
        }

        if (!CanUseEqualityOperator(context, isPattern.Expression))
            return;

        var constant = context.SemanticModel.GetConstantValue(constantPattern.Expression, context.CancellationToken);
        if (!constant.HasValue)
            return;

        // Equality operators never match NaN, while constant patterns (C# 11+) do, so
        // 'x is float.NaN' / 'x is double.NaN' must keep their pattern-matching semantics.
        if (IsNotANumber(constant.Value))
            return;

        var rule = negated ? IsNotConstantRule : IsConstantRule;
        context.ReportDiagnostic(Diagnostic.Create(rule, pattern.GetLocation()));
    }

    private static bool ResolvesToBuiltInNullOperator(SyntaxNodeAnalysisContext context, ExpressionSyntax expression)
    {
        var type = context.SemanticModel.GetTypeInfo(expression, context.CancellationToken).Type;
        if (type == null || type.TypeKind is TypeKind.Error or TypeKind.Unknown)
            return false;

        if (HasUserDefinedEqualityOperator(type))
            return false;

        return type.IsReferenceType || type.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
    }
}
