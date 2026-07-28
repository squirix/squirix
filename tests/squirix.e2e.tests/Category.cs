namespace Squirix.E2ETests;

/// <summary>Trait identifiers used to gate stress E2E tests so they are excluded from fast PR runs.</summary>
internal static class Category
{
    /// <summary>Trait name applied to every stress test class.</summary>
    public const string TraitName = "Suite";

    /// <summary>Trait value applied to every stress test class.</summary>
    public const string TraitValue = "Stress";
}
