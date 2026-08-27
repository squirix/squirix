using System;
using System.Collections.Generic;
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

    private static readonly LocalizableString MessageFormat =
        "Consecutive catch blocks for the same body should be combined into one 'when' filter pattern; " +
        "e.g. 'catch (Exception ex) when (ex is {0})'";

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
        if (catches.Count < 2)
            return;

        for (var start = 0; start < catches.Count; start++)
        {
            context.CancellationToken.ThrowIfCancellationRequested();

            if (!CanMerge(catches[start]))
                continue;

            var firstType = GetExceptionTypeName(catches[start]);
            if (firstType == null)
                continue;

            var runTypes = new List<string> { firstType };

            var end = start + 1;
            while (end < catches.Count && CanMerge(catches[end]) && BodiesAreEquivalent(catches[start], catches[end]))
            {
                context.CancellationToken.ThrowIfCancellationRequested();

                var type = GetExceptionTypeName(catches[end]);
                if (type == null || runTypes.Contains(type))
                    break;

                runTypes.Add(type);
                end++;
            }

            if (runTypes.Count >= 2)
            {
                var pattern = string.Join(" or ", runTypes);
                context.ReportDiagnostic(Diagnostic.Create(Rule, catches[start].CatchKeyword.GetLocation(), pattern));
            }

            start = end - 1;
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
