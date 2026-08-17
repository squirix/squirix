using System;

namespace Squirix.Attributes;

/// <summary>Marks a type as mutable, indicating it can be modified after construction.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface | AttributeTargets.Enum, Inherited = false)]
public sealed class MutableAttribute : SquirixAttribute;
