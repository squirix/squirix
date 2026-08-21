using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Squirix.Analyzers;

/// <summary>
/// Forbids xUnit <c>Assert.Throws</c>, <c>Assert.ThrowsAny</c>, <c>Assert.ThrowsAsync</c>,
/// and <c>Assert.ThrowsAnyAsync</c>. These assertions capture the operation in a delegate
/// display class and allocate it on every call; use the closure-free testkit assertions instead
/// (<c>ExceptionAssert.For&lt;T&gt;().Throws</c> / <c>NodeExceptionAssert.For&lt;T&gt;().Throws</c>
/// for synchronous throws, <c>AsyncAssert.ThrowsAsync</c> / <c>NodeAsyncAssert.ThrowsAsync</c>
/// for in-flight operations).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NoAllocatingXunitThrowsAssertAnalyzer : DiagnosticAnalyzer
{
    private const string DiagnosticId = "SQR019";

    private static readonly LocalizableString Description = "xUnit Throws assertions allocate a delegate display class on every call. " +
                                                             "Use the closure-free testkit asserts instead: ExceptionAssert.For<T>().Throws or " +
                                                             "NodeExceptionAssert.For<T>().Throws for synchronous throws, AsyncAssert.ThrowsAsync or " +
                                                             "NodeAsyncAssert.ThrowsAsync for in-flight operations.";

    private static readonly LocalizableString MessageFormat =
        "Do not use {0}; use the closure-free testkit exception asserts instead";

    private static readonly LocalizableString Title = "Avoid allocating xUnit Throws assertions";
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
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        var node = (InvocationExpressionSyntax)context.Node;

        if (node.Expression is not MemberAccessExpressionSyntax memberAccess)
            return;

        var name = memberAccess.Name.Identifier.Text;
        if (name is not ("Throws" or "ThrowsAny" or "ThrowsAsync" or "ThrowsAnyAsync"))
            return;

        if (context.SemanticModel.GetSymbolInfo(memberAccess, context.CancellationToken).Symbol is not IMethodSymbol method)
            return;

        var containingType = method.ContainingType;
        if (containingType?.Name != "Assert")
            return;

        var containingNamespace = containingType.ContainingNamespace;
        if (containingNamespace is null || containingNamespace.Name != "Xunit" || !containingNamespace.ContainingNamespace.IsGlobalNamespace)
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, node.GetLocation(), $"Assert.{name}"));
    }
}
