using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Flags consecutive <c language="csharp">catch</c> blocks that catch different exception types but
/// contain identical bodies. Such blocks read more clearly as a single
/// <c language="csharp">catch (Exception ex) when (ex is TOne or TTwo)</c> clause using pattern
/// matching, which keeps the duplicated handler body in one place (SQR020).
/// Only clauses without a <c language="csharp">when</c> filter whose exception variable is not
/// referenced in the body are considered, so the merge never changes behavior.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class MergeDuplicateCatchBlocksAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR020";

    private static readonly LocalizableString Description = "Consecutive catch blocks catch different exception types with identical bodies. " +
                                                            "Combine them into a single catch clause with a 'when' filter pattern, for example " +
                                                            "'catch (Exception ex) when (ex is IOException or ObjectDisposedException)', to keep the " +
                                                            "duplicated handler body in one place.";

    private static readonly LocalizableString MessageFormat = "Duplicate catch blocks for '{0}' and '{1}'; combine them with a 'when' filter pattern, e.g. " +
                                                              "'catch (Exception ex) when (ex is {0} or {1})'";

    private static readonly LocalizableString Title = "Merge duplicate catch blocks with identical bodies";

    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Usage", DiagnosticSeverity.Warning, true, Description);

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeTryStatement, SyntaxKind.TryStatement);
    }

    private static void AnalyzeTryStatement(SyntaxNodeAnalysisContext context)
    {
        var tryStatement = (TryStatementSyntax)context.Node;
        var catches = tryStatement.Catches;

        for (var i = 1; i < catches.Count; i++)
        {
            var previous = catches[i - 1];
            var current = catches[i];

            if (!CanMerge(previous) || !CanMerge(current))
                continue;

            var previousType = GetExceptionTypeName(previous);
            var currentType = GetExceptionTypeName(current);

            if (previousType == null || currentType == null || previousType == currentType)
                continue;

            if (!BodiesAreEquivalent(previous, current))
                continue;

            context.ReportDiagnostic(Diagnostic.Create(Rule, current.CatchKeyword.GetLocation(), currentType, previousType));
        }
    }

    /// <summary>
    /// A clause can only be offered for merging when it has a declared exception type, no
    /// <c language="csharp">when</c> filter, and does not reference its exception variable in the body.
    /// </summary>
    private static bool CanMerge(CatchClauseSyntax clause)
    {
        if (clause.Declaration == null)
            return false;

        if (clause.Filter != null)
            return false;

        return !ExceptionVariableIsReferenced(clause);
    }

    private static string? GetExceptionTypeName(CatchClauseSyntax clause)
    {
        var declaration = clause.Declaration;
        return declaration?.Type.ToString();
    }

    private static bool BodiesAreEquivalent(CatchClauseSyntax left, CatchClauseSyntax right) => left.Block.IsEquivalentTo(right.Block, false);

    private static bool ExceptionVariableIsReferenced(CatchClauseSyntax clause)
    {
        var identifier = clause.Declaration?.Identifier;
        if (identifier is null || identifier.Value.IsMissing || string.IsNullOrEmpty(identifier.Value.ValueText))
            return false;

        var name = identifier.Value.ValueText;
        foreach (var node in clause.Block.DescendantNodes())
        {
            if (node is IdentifierNameSyntax { Identifier.ValueText: var text } && text == name)
                return true;
        }

        return false;
    }
}
