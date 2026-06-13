using System;
using System.Collections.Generic;
using NetArchTest.Rules;
using TestResult = NetArchTest.Rules.TestResult;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// NetArchTest composition helpers used across architecture tests.
/// </summary>
internal static class ArchitectureNetArchRules
{
    /// <summary>
    /// Evaluates whether every type matched by <paramref name="matchingTypes" /> resides in one of the given exact namespaces (disjunction).
    /// </summary>
    /// <param name="matchingTypes">The NetArchTest predicate chain (for example <c>Types.InAssembly(...).That().HaveNameEndingWith("Options")</c>).</param>
    /// <param name="exactNamespaces">Exact namespace names; a type passes if its namespace equals any entry.</param>
    /// <returns>The NetArchTest result for the composed OR-of-namespace rule.</returns>
    public static TestResult EvaluateShouldResideInOneOfNamespaces(PredicateList matchingTypes, IReadOnlyList<string> exactNamespaces)
    {
        ArgumentNullException.ThrowIfNull(matchingTypes);
        ArgumentNullException.ThrowIfNull(exactNamespaces);
        if (exactNamespaces.Count == 0)
        {
            throw new ArgumentException("At least one namespace is required.", nameof(exactNamespaces));
        }

        var condition = matchingTypes.Should().ResideInNamespace(exactNamespaces[0]);
        for (var i = 1; i < exactNamespaces.Count; i++)
        {
            condition = condition.Or().ResideInNamespace(exactNamespaces[i]);
        }

        return condition.GetResult();
    }
}
