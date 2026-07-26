using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags outer loops whose block contains only a nested loop — braces must be omitted.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class OmitOuterLoopBracesAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR001";

    private static readonly LocalizableString Description = "When an outer loop's body is only a nested loop (no other statements), omit the outer braces.";

    private static readonly LocalizableString MessageFormat = "Omit braces from outer {0} when it only contains a nested loop";
    private static readonly LocalizableString Title = "Omit braces from outer loop that only contains a nested loop";
    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Style", DiagnosticSeverity.Error, true, Description);


    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.ForStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.ForEachStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.ForEachVariableStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.WhileStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.DoStatement);
    }

    private static void AnalyzeLoop(SyntaxNodeAnalysisContext context)
    {
        var body = GetLoopBody(context.Node);
        if (body is not BlockSyntax block)
            return;

        if (block.Statements.Count != 1)
            return;

        var only = block.Statements[0];
        if (!IsLoopStatement(only))
            return;

        var loopKind = GetLoopKindName(context.Node);
        context.ReportDiagnostic(Diagnostic.Create(Rule, block.OpenBraceToken.GetLocation(), loopKind));
    }

    private static StatementSyntax? GetLoopBody(SyntaxNode node)
    {
        return node.Kind() switch
        {
            SyntaxKind.ForStatement => ((ForStatementSyntax)node).Statement,
            SyntaxKind.ForEachStatement or SyntaxKind.ForEachVariableStatement => ((CommonForEachStatementSyntax)node).Statement,
            SyntaxKind.WhileStatement => ((WhileStatementSyntax)node).Statement,
            SyntaxKind.DoStatement => ((DoStatementSyntax)node).Statement,
            _ => null,
        };
    }

    private static string GetLoopKindName(SyntaxNode node)
    {
        return node.Kind() switch
        {
            SyntaxKind.ForStatement => "for",
            SyntaxKind.ForEachStatement or SyntaxKind.ForEachVariableStatement => "foreach",
            SyntaxKind.WhileStatement => "while",
            SyntaxKind.DoStatement => "do",
            _ => "loop",
        };
    }

    private static bool IsLoopStatement(StatementSyntax statement)
    {
        return statement.Kind() switch
        {
            SyntaxKind.ForStatement or SyntaxKind.ForEachStatement or SyntaxKind.ForEachVariableStatement or SyntaxKind.WhileStatement or SyntaxKind.DoStatement => true,
            _ => false,
        };
    }
}
