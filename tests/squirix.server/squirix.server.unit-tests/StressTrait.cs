namespace Squirix.Server.UnitTests;

/// <summary>Trait identifiers used to gate stress tests so they are excluded from fast PR runs.</summary>
internal static class StressTrait
{
    /// <summary>Trait name applied to every stress test class.</summary>
    public const string TraitName = "Suite";

    /// <summary>Trait value applied to every stress test class.</summary>
    public const string TraitValue = "Stress";
}
