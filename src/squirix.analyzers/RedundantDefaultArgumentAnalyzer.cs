using System;
using System.Collections.Immutable;
using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags call-site arguments that equal the parameter's default value (SQR011).
/// Matches Rider "The parameter '…' has the same default value".
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class RedundantDefaultArgumentAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR011";

    private static readonly LocalizableString Description = "Omit arguments that equal the parameter default; the default may change at the declaration.";

    private static readonly LocalizableString MessageFormat = "The parameter '{0}' has the same default value";

    private static readonly LocalizableString Title = "Avoid redundant default argument values";
    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Style", DiagnosticSeverity.Warning, true, Description);


    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ObjectCreationExpression);
        context.RegisterSyntaxNodeAction(AnalyzeImplicitObjectCreation, SyntaxKind.ImplicitObjectCreationExpression);
    }

    private static void AnalyzeArgumentList(SyntaxNodeAnalysisContext context, ArgumentListSyntax argumentList, IMethodSymbol method,
        Func<SyntaxNode, ArgumentListSyntax, ExpressionSyntax> withArgumentList)
    {
        var parameters = method.Parameters;
        if (parameters.Length == 0 || argumentList.Arguments.Count == 0)
            return;

        // Map each argument index to a parameter index (positional + named).
        var argumentToParameter = new int[argumentList.Arguments.Count];
        var nextPositional = 0;
        for (var i = 0; i < argumentList.Arguments.Count; i++)
        {
            var argument = argumentList.Arguments[i];
            if (argument.NameColon == null)
            {
                if (nextPositional >= parameters.Length)
                    return;

                // params absorbs remaining positionals.
                if (parameters[nextPositional].IsParams)
                {
                    argumentToParameter[i] = nextPositional;
                    continue;
                }

                argumentToParameter[i] = nextPositional;
                nextPositional++;
                continue;
            }

            var name = argument.NameColon.Name.Identifier.ValueText;
            var parameterIndex = FindParameterIndex(parameters, name);
            if (parameterIndex < 0)
                return;

            argumentToParameter[i] = parameterIndex;
            if (parameterIndex >= nextPositional && !parameters[parameterIndex].IsParams)
                nextPositional = parameterIndex + 1;
        }

        for (var i = 0; i < argumentList.Arguments.Count; i++)
        {
            var parameterIndex = argumentToParameter[i];
            var parameter = parameters[parameterIndex];
            if (parameter.IsParams || !parameter.IsOptional)
                continue;

            if (!TryGetParameterDefault(parameter, out var defaultValue))
                continue;

            var argument = argumentList.Arguments[i];
            if (!ArgumentEqualsDefault(context, argument.Expression, defaultValue))
                continue;

            // Named args can always be dropped. Positional args only when every later
            // argument is also a redundant optional default (otherwise binding shifts).
            if (argument.NameColon == null && !TrailingDefaultsCanBeOmitted(argumentList, argumentToParameter, parameters, i, context))
                continue;

            if (!RemainsBoundAfterRemoving(context, argumentList, i, withArgumentList, method))
                continue;

            context.ReportDiagnostic(Diagnostic.Create(Rule, argument.GetLocation(), parameter.Name));
        }
    }

    private static void AnalyzeImplicitObjectCreation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ImplicitObjectCreationExpressionSyntax creation)
            return;

        if (context.SemanticModel.GetSymbolInfo(creation, context.CancellationToken).Symbol is not IMethodSymbol method)
            return;

        AnalyzeArgumentList(context, creation.ArgumentList, method, static (node, list) => ((ImplicitObjectCreationExpressionSyntax)node).WithArgumentList(list));
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        if (context.SemanticModel.GetSymbolInfo(invocation, context.CancellationToken).Symbol is not IMethodSymbol method)
            return;

        AnalyzeArgumentList(context, invocation.ArgumentList, method, static (node, list) => ((InvocationExpressionSyntax)node).WithArgumentList(list));
    }

    private static void AnalyzeObjectCreation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ObjectCreationExpressionSyntax creation || creation.ArgumentList == null)
            return;

        if (context.SemanticModel.GetSymbolInfo(creation, context.CancellationToken).Symbol is not IMethodSymbol method)
            return;

        AnalyzeArgumentList(context, creation.ArgumentList, method, static (node, list) => ((ObjectCreationExpressionSyntax)node).WithArgumentList(list));
    }

    private static bool ArgumentEqualsDefault(SyntaxNodeAnalysisContext context, ExpressionSyntax expression, object? defaultValue)
    {
        // default / default(T) matches any default value including null.
        if (expression.IsKind(SyntaxKind.DefaultLiteralExpression) || expression.IsKind(SyntaxKind.DefaultExpression))
            return true;

        var constant = context.SemanticModel.GetConstantValue(expression, context.CancellationToken);
        if (!constant.HasValue)
            return false;

        return EqualsNormalized(constant.Value, defaultValue);
    }

    private static bool EqualsNormalized(object? left, object? right)
    {
        if (Equals(left, right))
            return true;

        // Roslyn may box enum defaults as the underlying integral type.
        if (left is Enum leftEnum && right != null)
        {
            return Equals(Convert.ChangeType(leftEnum, Enum.GetUnderlyingType(leftEnum.GetType()), CultureInfo.InvariantCulture), right);
        }

        if (right is Enum rightEnum && left != null)
        {
            return Equals(left, Convert.ChangeType(rightEnum, Enum.GetUnderlyingType(rightEnum.GetType()), CultureInfo.InvariantCulture));
        }

        return false;
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

    private static object? GetNumericZero(SpecialType specialType)
    {
        return specialType switch
        {
            SpecialType.System_Boolean => false,
            SpecialType.System_Byte => (byte)0,
            SpecialType.System_SByte => (sbyte)0,
            SpecialType.System_Int16 => (short)0,
            SpecialType.System_UInt16 => (ushort)0,
            SpecialType.System_Int32 => 0,
            SpecialType.System_UInt32 => 0u,
            SpecialType.System_Int64 => 0L,
            SpecialType.System_UInt64 => 0UL,
            SpecialType.System_Single => 0f,
            SpecialType.System_Double => 0d,
            SpecialType.System_Decimal => 0m,
            SpecialType.System_Char => '\0',
            SpecialType.System_IntPtr => IntPtr.Zero,
            SpecialType.System_UIntPtr => UIntPtr.Zero,
            _ => null,
        };
    }

    private static object? GetValueTypeDefault(ITypeSymbol type)
    {
        if (type.TypeKind is not TypeKind.Enum || type is not INamedTypeSymbol enumType)
            return GetNumericZero(type.SpecialType);
        // Enum default is 0 converted to the underlying type's zero value.
        var underlying = enumType.EnumUnderlyingType;
        if (underlying == null)
            return 0;

        return GetNumericZero(underlying.SpecialType);
    }

    private static bool HasOptionalAttribute(IParameterSymbol parameter)
    {
        var attributes = parameter.GetAttributes();
        for (var i = 0; i < attributes.Length; i++)
        {
            var attr = attributes[i].AttributeClass;
            if (attr?.Name is "OptionalAttribute" && attr.ContainingNamespace?.ToDisplayString() is "System.Runtime.InteropServices")
            {
                return true;
            }
        }

        return false;
    }

    private static bool RemainsBoundAfterRemoving(SyntaxNodeAnalysisContext context, ArgumentListSyntax argumentList, int argumentIndex,
        Func<SyntaxNode, ArgumentListSyntax, ExpressionSyntax> withArgumentList, IMethodSymbol method)
    {
        var rewrittenArgs = argumentList.Arguments.RemoveAt(argumentIndex);
        var rewrittenList = argumentList.WithArguments(rewrittenArgs);
        var rewrittenCall = withArgumentList(context.Node, rewrittenList);

        var speculative = context.SemanticModel.GetSpeculativeSymbolInfo(context.Node.SpanStart, rewrittenCall, SpeculativeBindingOption.BindAsExpression);

        return speculative.Symbol is IMethodSymbol speculativeMethod && SymbolEqualityComparer.Default.Equals(speculativeMethod.OriginalDefinition, method.OriginalDefinition);
    }

    private static bool TrailingDefaultsCanBeOmitted(ArgumentListSyntax argumentList, int[] argumentToParameter, ImmutableArray<IParameterSymbol> parameters, int startIndex,
        SyntaxNodeAnalysisContext context)
    {
        for (var i = startIndex; i < argumentList.Arguments.Count; i++)
        {
            if (argumentList.Arguments[i].NameColon != null)
                return false;

            var parameter = parameters[argumentToParameter[i]];
            if (parameter.IsParams || !parameter.IsOptional || !TryGetParameterDefault(parameter, out var defaultValue))
                return false;

            if (!ArgumentEqualsDefault(context, argumentList.Arguments[i].Expression, defaultValue))
                return false;
        }

        return true;
    }

    private static bool TryGetParameterDefault(IParameterSymbol parameter, out object? defaultValue)
    {
        if (parameter.HasExplicitDefaultValue)
        {
            defaultValue = parameter.ExplicitDefaultValue;
            return true;
        }

        // [Optional] without C# default uses the type's default value.
        if (HasOptionalAttribute(parameter))
        {
            defaultValue = parameter.Type.IsReferenceType || parameter.Type is IPointerTypeSymbol ? null : GetValueTypeDefault(parameter.Type);
            return true;
        }

        defaultValue = null;
        return false;
    }
}
