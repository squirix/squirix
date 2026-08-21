using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Requires braces when an if/else-if/else body is a single embedded statement that spans multiple lines.
/// Nested if statements without braces are exempt; the leaf if is analyzed on its own node so the brace is
/// reported at the actual multiline statement (mirrors SQR008 for loops).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class RequireMultilineIfBodyBracesAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR018";

    private static readonly LocalizableString Description =
        "When an if/else-if/else body is a single embedded statement that spans multiple lines, add braces around the body.";

    private static readonly LocalizableString MessageFormat = "Add braces to {0} body when the embedded statement spans multiple lines";
    private static readonly LocalizableString Title = "Add braces to multiline embedded if/else body";
    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Style", DiagnosticSeverity.Error, true, Description);

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterSyntaxNodeAction(AnalyzeIf, SyntaxKind.IfStatement);
    }

    private static void AnalyzeIf(SyntaxNodeAnalysisContext context)
    {
        var ifStatement = (IfStatementSyntax)context.Node;

        // Analyze each if/else-if chain once from its root.
        if (ifStatement.Parent is ElseClauseSyntax)
            return;

        var current = ifStatement;
        var isElseIf = false;
        while (true)
        {
            CheckBranch(context, current.Statement, isElseIf ? "else if" : "if");

            if (current.Else == null)
                return;

            if (current.Else.Statement is IfStatementSyntax elseIf)
            {
                current = elseIf;
                isElseIf = true;
                continue;
            }

            CheckBranch(context, current.Else.Statement, "else");
            return;
        }
    }

    private static void CheckBranch(SyntaxNodeAnalysisContext context, StatementSyntax body, string kind)
    {
        if (body is BlockSyntax)
            return;

        // A nested `if` without braces is analyzed on its own node; report the brace at its leaf statement.
        if (body is IfStatementSyntax)
            return;

        if (!LoopStatementSyntaxHelpers.SpansMultipleLines(body))
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, body.GetFirstToken().GetLocation(), kind));
    }
}
