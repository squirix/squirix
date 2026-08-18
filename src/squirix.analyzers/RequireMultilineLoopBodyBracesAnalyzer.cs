using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Requires braces when a loop body is a single embedded statement that spans multiple lines,
/// except when that statement is a nested loop.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class RequireMultilineLoopBodyBracesAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR008";

    private static readonly LocalizableString Description =
        "When a loop body is a single embedded statement that spans multiple lines, add braces around the body. Nested loops alone are exempt.";

    private static readonly LocalizableString MessageFormat = "Add braces to {0} body when the embedded statement spans multiple lines";
    private static readonly LocalizableString Title = "Add braces to multiline embedded loop body";
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

        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.ForStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.ForEachStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.ForEachVariableStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.WhileStatement);
        context.RegisterSyntaxNodeAction(AnalyzeLoop, SyntaxKind.DoStatement);
    }

    private static void AnalyzeLoop(SyntaxNodeAnalysisContext context)
    {
        var body = LoopStatementSyntaxHelpers.GetLoopBody(context.Node);
        if (body is null or BlockSyntax)
            return;

        if (LoopStatementSyntaxHelpers.IsLoopStatement(body))
            return;

        if (!LoopStatementSyntaxHelpers.SpansMultipleLines(body))
            return;

        var loopKind = LoopStatementSyntaxHelpers.GetLoopKindName(context.Node);
        context.ReportDiagnostic(Diagnostic.Create(Rule, body.GetFirstToken().GetLocation(), loopKind));
    }
}
