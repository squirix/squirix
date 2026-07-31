using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags in-order named arguments that can be positional (SQR009).
/// Matches Rider "Inconsistent argument style: redundant name identifier".
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class RedundantNamedArgumentAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR009";

    private static readonly LocalizableString Description =
        "Prefer positional arguments; omit argument names when the call uses parameters in declaration order.";

    private static readonly LocalizableString MessageFormat =
        "Named argument '{0}' is redundant; use a positional argument";

    private static readonly LocalizableString Title = "Avoid redundant named arguments";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId,
        Title,
        MessageFormat,
        "Style",
        DiagnosticSeverity.Warning,
        true,
        Description);

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ObjectCreationExpression);
        context.RegisterSyntaxNodeAction(AnalyzeImplicitObjectCreation, SyntaxKind.ImplicitObjectCreationExpression);
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        if (context.SemanticModel.GetSymbolInfo(invocation, context.CancellationToken).Symbol is not IMethodSymbol symbol)
            return;

        AnalyzeArgumentList(
            context,
            invocation.ArgumentList,
            symbol,
            static (node, list) => ((InvocationExpressionSyntax)node).WithArgumentList(list));
    }

    private static void AnalyzeObjectCreation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ObjectCreationExpressionSyntax creation || creation.ArgumentList is null)
            return;

        if (context.SemanticModel.GetSymbolInfo(creation, context.CancellationToken).Symbol is not IMethodSymbol symbol)
            return;

        AnalyzeArgumentList(
            context,
            creation.ArgumentList,
            symbol,
            static (node, list) => ((ObjectCreationExpressionSyntax)node).WithArgumentList(list));
    }

    private static void AnalyzeImplicitObjectCreation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ImplicitObjectCreationExpressionSyntax creation)
            return;

        if (context.SemanticModel.GetSymbolInfo(creation, context.CancellationToken).Symbol is not IMethodSymbol symbol)
            return;

        AnalyzeArgumentList(
            context,
            creation.ArgumentList,
            symbol,
            static (node, list) => ((ImplicitObjectCreationExpressionSyntax)node).WithArgumentList(list));
    }

    private static void AnalyzeArgumentList(
        SyntaxNodeAnalysisContext context,
        ArgumentListSyntax argumentList,
        IMethodSymbol method,
        Func<SyntaxNode, ArgumentListSyntax, ExpressionSyntax> withArgumentList)
    {
        // Reduced extension invocations already exclude the receiver from Parameters.
        var parameters = method.Parameters;
        if (parameters.Length is 0 || argumentList.Arguments.Count is 0)
            return;

        var expectedIndex = 0;
        for (var i = 0; i < argumentList.Arguments.Count; i++)
        {
            var argument = argumentList.Arguments[i];
            if (argument.NameColon is null)
            {
                expectedIndex++;
                continue;
            }

            var name = argument.NameColon.Name.Identifier.ValueText;
            var parameterIndex = FindParameterIndex(parameters, name);
            if (parameterIndex < 0)
                return;

            // Gap or reordering: C# requires names for the rest of the list.
            if (parameterIndex != expectedIndex)
                return;

            if (RemainsBoundToSameMethod(context, argumentList, i, withArgumentList, method))
            {
                context.ReportDiagnostic(
                    Diagnostic.Create(Rule, argument.NameColon.GetLocation(), name));
            }

            expectedIndex++;
        }
    }

    private static bool RemainsBoundToSameMethod(
        SyntaxNodeAnalysisContext context,
        ArgumentListSyntax argumentList,
        int argumentIndex,
        Func<SyntaxNode, ArgumentListSyntax, ExpressionSyntax> withArgumentList,
        IMethodSymbol method)
    {
        var argument = argumentList.Arguments[argumentIndex];
        var positional = argument.WithNameColon(null);
        var rewrittenList = argumentList.WithArguments(argumentList.Arguments.Replace(argument, positional));
        var rewrittenCall = withArgumentList(context.Node, rewrittenList);

        var speculative = context.SemanticModel.GetSpeculativeSymbolInfo(
            context.Node.SpanStart,
            rewrittenCall,
            SpeculativeBindingOption.BindAsExpression);

        return speculative.Symbol is IMethodSymbol speculativeMethod
            && SymbolEqualityComparer.Default.Equals(speculativeMethod.OriginalDefinition, method.OriginalDefinition);
    }

    private static int FindParameterIndex(ImmutableArray<IParameterSymbol> parameters, string name)
    {
        for (var i = 0; i < parameters.Length; i++)
        {
            if (string.Equals(parameters[i].Name, name, StringComparison.Ordinal))
                return i;
        }

        return -1;
    }
}
