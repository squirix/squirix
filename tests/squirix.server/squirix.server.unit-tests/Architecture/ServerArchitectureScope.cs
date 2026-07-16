using ArchUnitNET.Fluent.Syntax.Elements.Types;
using static ArchUnitNET.Fluent.ArchRuleDefinition;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>ArchUnitNET scopes for types compiled into <c>Squirix.Server</c>.</summary>
internal static class ServerArchitectureScope
{
    internal static GivenTypesConjunction Server =>
        Types().That().HaveFullNameContaining(ServerArchitectureNamespaces.Root);
}
