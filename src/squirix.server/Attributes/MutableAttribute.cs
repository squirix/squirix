using System;

namespace Squirix.Server.Attributes;

/// <summary>Marks a type as mutable, indicating it can be modified after construction.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface | AttributeTargets.Enum, Inherited = false)]
internal sealed class MutableAttribute : SquirixAttribute;
