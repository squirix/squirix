using System;

namespace Squirix.Attributes;

/// <summary>Marks a type whose mutable state is protected for concurrent access.</summary>
/// <remarks>Use this marker for synchronization wrappers whose accessors provide the memory barriers.</remarks>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface, Inherited = false)]
public sealed class ThreadSafeAttribute : SquirixAttribute;
