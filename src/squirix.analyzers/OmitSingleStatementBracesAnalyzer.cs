using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags braces around a single-line single embedded statement that should be omitted (SQR010).
/// Complements IDE0011, which does not reliably enforce <c>csharp_prefer_braces = false</c> on build.
/// Keeps braces when the statement spans multiple lines, or when an if/else chain has a multi-statement branch (SA1520).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class OmitSingleStatementBracesAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR010";

    private static readonly LocalizableString Description =
        "Omit braces when a control-flow body is a single single-line statement. Keep braces for multiline bodies and for if/else chains with a multi-statement branch.";

    private static readonly LocalizableString MessageFormat = "Omit braces from '{0}' when the body is a single single-line statement";

    private static readonly LocalizableString Title = "Omit braces from single-line single-statement body";
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
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.ForStatement, SyntaxKind.ForEachStatement, SyntaxKind.ForEachVariableStatement, SyntaxKind.WhileStatement,
            SyntaxKind.DoStatement);
        context.RegisterSyntaxNodeAction(AnalyzeUsing, SyntaxKind.UsingStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLock, SyntaxKind.LockStatement);
        context.RegisterSyntaxNodeAction(AnalyzeFixed, SyntaxKind.FixedStatement);
    }

    private static void AnalyzeFixed(SyntaxNodeAnalysisContext context)
    {
        var statement = (FixedStatementSyntax)context.Node;
        ReportSimpleEmbedded(context, statement.Statement, "fixed");
    }

    private static void AnalyzeIf(SyntaxNodeAnalysisContext context)
    {
        var ifStatement = (IfStatementSyntax)context.Node;

        // Analyze each if/else-if chain once from its root.
        if (ifStatement.Parent is ElseClauseSyntax)
            return;

        if (!ChainAllowsOmittingBraces(ifStatement))
            return;

        var current = ifStatement;
        while (true)
        {
            ReportIfBlock(context, current.Statement, "if");

            if (current.Else == null)
                return;

            if (current.Else.Statement is not IfStatementSyntax elseIf)
            {
                ReportIfBlock(context, current.Else.Statement, "else");
                return;
            }

            current = elseIf;
        }
    }

    private static void AnalyzeLock(SyntaxNodeAnalysisContext context)
    {
        var statement = (LockStatementSyntax)context.Node;
        ReportSimpleEmbedded(context, statement.Statement, "lock");
    }

    private static void AnalyzeLoop(SyntaxNodeAnalysisContext context)
    {
        var body = LoopStatementSyntaxHelpers.GetLoopBody(context.Node);
        if (body is not BlockSyntax block || block.Statements.Count != 1)
            return;

        var only = block.Statements[0];

        // Nested-loop outer braces are SQR001. Multiline non-loop bodies must keep braces (SQR008).
        if (LoopStatementSyntaxHelpers.IsLoopStatement(only))
            return;

        if (LoopStatementSyntaxHelpers.SpansMultipleLines(only))
            return;

        var kind = LoopStatementSyntaxHelpers.GetLoopKindName(context.Node);
        context.ReportDiagnostic(Diagnostic.Create(Rule, block.OpenBraceToken.GetLocation(), kind));
    }

    private static void AnalyzeUsing(SyntaxNodeAnalysisContext context)
    {
        var statement = (UsingStatementSyntax)context.Node;
        ReportSimpleEmbedded(context, statement.Statement, "using");
    }

    private static bool ChainAllowsOmittingBraces(IfStatementSyntax ifStatement)
    {
        // All branches must be single-line single statements so stripping braces cannot
        // leave a multiline braced sibling next to an unbraced branch (SA1520).
        var current = ifStatement;
        while (true)
        {
            if (!IsOmittableSingleLineBody(current.Statement))
                return false;

            if (current.Else == null)
                return true;

            if (current.Else.Statement is not IfStatementSyntax elseIf)
                return IsOmittableSingleLineBody(current.Else.Statement);

            current = elseIf;
        }
    }

    private static bool IsOmittableSingleLineBody(StatementSyntax statement)
    {
        if (statement is not BlockSyntax block)
            return !LoopStatementSyntaxHelpers.SpansMultipleLines(statement);
        if (block.Statements.Count != 1)
            return false;

        return !LoopStatementSyntaxHelpers.SpansMultipleLines(block.Statements[0]);
    }

    private static void ReportIfBlock(SyntaxNodeAnalysisContext context, StatementSyntax body, string kind)
    {
        if (body is not BlockSyntax block || block.Statements.Count != 1)
            return;

        if (LoopStatementSyntaxHelpers.SpansMultipleLines(block.Statements[0]))
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, block.OpenBraceToken.GetLocation(), kind));
    }

    private static void ReportSimpleEmbedded(SyntaxNodeAnalysisContext context, StatementSyntax body, string kind)
    {
        if (body is not BlockSyntax block || block.Statements.Count != 1)
            return;

        // using/lock/fixed chains may nest; only flag a block wrapping a non-using/lock/fixed single statement
        // when that statement is not itself another resource statement that prefers shared braces.
        var only = block.Statements[0];
        if (only.IsKind(SyntaxKind.UsingStatement) || only.IsKind(SyntaxKind.LockStatement) || only.IsKind(SyntaxKind.FixedStatement))
            return;

        if (LoopStatementSyntaxHelpers.SpansMultipleLines(only))
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, block.OpenBraceToken.GetLocation(), kind));
    }
}
