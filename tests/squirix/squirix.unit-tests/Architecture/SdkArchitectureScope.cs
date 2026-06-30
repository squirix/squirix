using NetArchTest.Rules;

namespace Squirix.UnitTests.Architecture;

/// <summary>NetArchTest scopes that avoid runtime reflection in test code.</summary>
internal static class SdkArchitectureScope
{
    internal static PredicateList Sdk => Types.InCurrentDomain().That().ResideInNamespaceStartingWith("Squirix.").And().DoNotResideInNamespaceStartingWith("Squirix.Server");
}
