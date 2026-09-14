using System;

namespace Squirix.Server.Attributes;

/// <summary>Marks a type as immutable, indicating it should not be modified after construction.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface | AttributeTargets.Enum, Inherited = false)]
internal sealed class ImmutableAttribute : SquirixAttribute;
