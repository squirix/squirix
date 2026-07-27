using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Squirix.Analyzers;

internal static class LoopStatementSyntaxHelpers
{
    internal static StatementSyntax? GetLoopBody(SyntaxNode node)
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

    internal static string GetLoopKindName(SyntaxNode node)
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

    internal static bool IsLoopStatement(StatementSyntax statement)
    {
        return statement.Kind() switch
        {
            SyntaxKind.ForStatement or SyntaxKind.ForEachStatement or SyntaxKind.ForEachVariableStatement or SyntaxKind.WhileStatement or SyntaxKind.DoStatement => true,
            _ => false,
        };
    }

    internal static bool SpansMultipleLines(SyntaxNode node)
    {
        var tree = node.SyntaxTree;
        var lineSpan = tree.GetLineSpan(node.Span);
        return lineSpan.StartLinePosition.Line != lineSpan.EndLinePosition.Line;
    }
}
