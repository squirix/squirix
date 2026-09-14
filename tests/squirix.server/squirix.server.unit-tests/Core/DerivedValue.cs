using JetBrains.Annotations;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Derived value for testing runtime type preservation.</summary>
[Immutable]
internal sealed record DerivedValue : IValueContract
{
    [UsedImplicitly]
    public string? DerivedField { get; init; }
}
