using JetBrains.Annotations;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Derived value for journal test.</summary>
[Immutable]
internal sealed record DerivedValue : IValueContract
{
    [UsedImplicitly]
    public string? DerivedField { get; init; }
}
