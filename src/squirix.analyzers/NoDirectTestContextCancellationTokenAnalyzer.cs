using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Forbids direct use of <c>TestContext.Current.CancellationToken</c>.
/// It may only be written inside a type that declares a <c>DefaultCancellationToken</c> member
/// (the base test classes), so every other test consumes <c>DefaultCancellationToken</c> instead.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NoDirectTestContextCancellationTokenAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR017";

    private static readonly LocalizableString Description = "TestContext.Current.CancellationToken must not be used directly outside the base test classes. " +
                                                            "Declare a DefaultCancellationToken member in the base class and consume it from derived tests.";

    private static readonly LocalizableString MessageFormat = "Do not use TestContext.Current.CancellationToken directly; use DefaultCancellationToken instead";

    private static readonly LocalizableString Title = "Avoid direct use of TestContext.Current.CancellationToken";
    private static readonly DiagnosticDescriptor Rule = new(DiagnosticId, Title, MessageFormat, "Usage", DiagnosticSeverity.Error, true, Description);


    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeMemberAccess, SyntaxKind.SimpleMemberAccessExpression);
    }

    private static void AnalyzeMemberAccess(SyntaxNodeAnalysisContext context)
    {
        var node = (MemberAccessExpressionSyntax)context.Node;

        if (!IsTestContextCancellationToken(node))
            return;

        var typeDeclaration = GetEnclosingType(node);
        if (typeDeclaration is null)
            return;

        var symbol = context.SemanticModel.GetDeclaredSymbol(typeDeclaration, context.CancellationToken);
        if (symbol is null)
            return;

        if (DeclaresDefaultCancellationToken(symbol))
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, node.GetLocation()));
    }

    private static bool DeclaresDefaultCancellationToken(INamedTypeSymbol symbol)
    {
        foreach (var member in symbol.GetMembers("DefaultCancellationToken"))
        {
            if (member is IPropertySymbol or IFieldSymbol)
                return true;
        }

        return false;
    }

    private static TypeDeclarationSyntax? GetEnclosingType(MemberAccessExpressionSyntax node)
    {
        for (var current = node.Parent; current is not null; current = current.Parent)
        {
            if (current is TypeDeclarationSyntax type)
                return type;
        }

        return null;
    }

    private static bool IsTestContextCancellationToken(MemberAccessExpressionSyntax node)
    {
        if (node.Name.Identifier.Text != "CancellationToken")
            return false;

        if (node.Expression is not MemberAccessExpressionSyntax currentAccess)
            return false;

        if (currentAccess.Name.Identifier.Text != "Current")
            return false;

        return currentAccess.Expression is IdentifierNameSyntax { Identifier.Text: "TestContext" };
    }
}
