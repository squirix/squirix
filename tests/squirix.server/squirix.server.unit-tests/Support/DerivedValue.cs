using System.Text.Json.Serialization;
using JetBrains.Annotations;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Derived value for testing runtime type preservation.</summary>
[Immutable]
internal sealed record DerivedValue : IValueContract
{
    [UsedImplicitly]
    [JsonInclude]
    internal string? DerivedField { get; init; }
}
