using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Enforces the Squirix dispose-flag convention: a disposure guard must be an <c language="csharp">int</c> flag
/// toggled through <see cref="System.Threading.Interlocked" /> (or observed through
/// <see cref="System.Threading.Volatile" />), never a plain <c language="csharp">bool</c> field.
/// <list type="bullet">
///     <item>
///         <description>SQR015: flags a <c language="csharp">bool</c> field whose name contains "disposed".</description>
///     </item>
///     <item>
///         <description>SQR016: flags an <c language="csharp">int</c> "disposed" flag accessed outside Interlocked/Volatile.</description>
///     </item>
/// </list>
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NoBoolDisposedFieldAnalyzer : DiagnosticAnalyzer
{
    private const string BoolRuleId = "SQR015";
    private const string Category = "Concurrency";
    private const string IntRuleId = "SQR016";

    private static readonly LocalizableString BoolDescription =
        "A plain bool _disposed field is not thread-safe. Squirix requires an int flag mutated via System.Threading.Interlocked and observed via System.Threading.Volatile.";

    private static readonly LocalizableString BoolMessage = "Field '{0}' is a bool dispose guard; use 'private int {0};' toggled with Interlocked.Exchange";

    private static readonly LocalizableString BoolTitle = "Dispose guard must be an int flag toggled with Interlocked";

    private static readonly LocalizableString IntDescription =
        "An int dispose flag must only be mutated via System.Threading.Interlocked and observed via System.Threading.Volatile to avoid torn reads and missing volatile semantics.";

    private static readonly LocalizableString IntMessage =
        "Dispose flag '{0}' is read or written without Interlocked/Volatile; use Interlocked.Exchange/CompareExchange or Volatile.Read/Write";

    private static readonly LocalizableString IntTitle = "Dispose guard must be accessed through Interlocked or Volatile";
    private static readonly DiagnosticDescriptor BoolRule = new(BoolRuleId, BoolTitle, BoolMessage, Category, DiagnosticSeverity.Warning, true, BoolDescription);


    private static readonly DiagnosticDescriptor IntRule = new(IntRuleId, IntTitle, IntMessage, Category, DiagnosticSeverity.Warning, true, IntDescription);


    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [BoolRule, IntRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeFieldSymbol, SymbolKind.Field);
        context.RegisterSyntaxNodeAction(AnalyzeFieldAccess, SyntaxKind.IdentifierName);
    }

    private static void AnalyzeFieldAccess(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not IdentifierNameSyntax)
            return;

        if (context.SemanticModel.GetSymbolInfo(context.Node, context.CancellationToken).Symbol is not IFieldSymbol symbol)
            return;

        if (AnalyzerHelpers.IsCompilerOrGenerated(symbol))
            return;

        if (!IsDisposedFieldName(symbol.Name))
            return;

        if (symbol.Type.SpecialType != SpecialType.System_Int32)
            return;

        if (IsGuardedByInterlockedOrVolatile(context.Node))
            return;

        context.ReportDiagnostic(Diagnostic.Create(IntRule, context.Node.GetLocation(), symbol.Name));
    }

    private static void AnalyzeFieldSymbol(SymbolAnalysisContext context)
    {
        var field = (IFieldSymbol)context.Symbol;
        if (AnalyzerHelpers.IsCompilerOrGenerated(field))
            return;

        if (field.Type.SpecialType != SpecialType.System_Boolean)
            return;

        if (!IsDisposedFieldName(field.Name))
            return;

        context.ReportDiagnostic(Diagnostic.Create(BoolRule, field.Locations.IsDefaultOrEmpty ? Location.None : field.Locations[0], field.Name));
    }

    private static bool IsDisposedFieldName(string name) => name.Equals("_disposed", StringComparison.OrdinalIgnoreCase);

    private static bool IsGuardedByInterlockedOrVolatile(SyntaxNode node)
    {
        for (var current = node.Parent; current != null; current = current.Parent)
        {
            if (current is InvocationExpressionSyntax invocation)
            {
                if (IsInterlockedOrVolatileInvocation(invocation, node))
                    return true;

                // Do not look past an unrelated invocation boundary.
                return false;
            }

            if (current is StatementSyntax)
                break;
        }

        return false;
    }

    private static bool IsInterlockedOrVolatileInvocation(InvocationExpressionSyntax invocation, SyntaxNode node)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax member)
            return false;

        if (member.Expression is not IdentifierNameSyntax receiver)
            return false;

        var receiverName = receiver.Identifier.ValueText;
        if (!receiverName.StartsWith("Interlocked", StringComparison.OrdinalIgnoreCase) && !receiverName.StartsWith("Volatile", StringComparison.OrdinalIgnoreCase))
            return false;

        foreach (var argument in invocation.ArgumentList.Arguments)
        {
            if (IsWithin(argument.Expression, node))
                return true;
        }

        return false;
    }

    private static bool IsWithin(SyntaxNode ancestor, SyntaxNode descendant)
    {
        for (var current = descendant; current != null; current = current.Parent)
        {
            if (current == ancestor)
                return true;
        }

        return false;
    }
}
