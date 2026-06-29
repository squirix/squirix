using NetArchTest.Rules;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>NetArchTest scopes that avoid runtime reflection in test code.</summary>
internal static class ArchitectureTypeScope
{
    internal static PredicateList Server => Types.InCurrentDomain().That().ResideInNamespaceStartingWith("Squirix.Server");
}
