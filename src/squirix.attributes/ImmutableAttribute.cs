using System;

namespace Squirix.Attributes;

/// <summary>Maps a cache value to the compact value-only gRPC wire form.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface | AttributeTargets.Enum, Inherited = false)]
public sealed class ImmutableAttribute : Attribute;
