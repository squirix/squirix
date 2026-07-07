using System;
using System.Collections.Generic;
using ArchUnitNET.Fluent.Syntax.Elements.Types;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>ArchUnitNET composition helpers used across architecture tests.</summary>
internal static class ArchitectureRuleHelpers
{
    /// <summary>
    /// Asserts every type matched by <paramref name="matchingTypes" /> resides in one of the given exact namespaces (disjunction).
    /// </summary>
    /// <param name="matchingTypes">The ArchUnitNET type predicate (for example name suffix <c>Options</c>).</param>
    /// <param name="exactNamespaces">Exact namespace names; a type passes if its namespace equals any entry.</param>
    /// <exception cref="ArgumentNullException"><paramref name="matchingTypes"/> or <paramref name="exactNamespaces"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="exactNamespaces"/> is empty.</exception>
    public static void AssertResideInOneOfNamespaces(GivenTypesConjunction matchingTypes, IReadOnlyList<string> exactNamespaces)
    {
        ArgumentNullException.ThrowIfNull(matchingTypes);
        ArgumentNullException.ThrowIfNull(exactNamespaces);
        if (exactNamespaces.Count is 0)
        {
            throw new ArgumentException("At least one namespace is required.", nameof(exactNamespaces));
        }

        foreach (var type in matchingTypes.GetObjects(ServerArchitecture.Instance))
        {
            var namespaceName = type.Namespace?.FullName ?? string.Empty;
            Assert.True(
                ResidesInOneOfExactNamespaces(namespaceName, exactNamespaces),
                $"{type.FullName} resides in '{namespaceName}', expected one of [{string.Join(", ", exactNamespaces)}].");
        }
    }

    private static bool ResidesInOneOfExactNamespaces(string typeNamespace, IReadOnlyList<string> exactNamespaces)
    {
        for (var namespaceIndex = 0; namespaceIndex < exactNamespaces.Count; namespaceIndex++)
        {
            if (string.Equals(typeNamespace, exactNamespaces[namespaceIndex], StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
