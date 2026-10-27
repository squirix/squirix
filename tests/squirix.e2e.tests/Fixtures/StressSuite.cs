namespace Squirix.E2ETests.Fixtures;

/// <summary>The trait that keeps a test out of the pull request suites and in the stress job; the same trait <c language="csharp">Category</c> names for tests outside the multi-node area.</summary>
internal static class StressSuite
{
    /// <summary>The trait name.</summary>
    internal const string TraitName = "Suite";

    /// <summary>The trait value.</summary>
    internal const string TraitValue = "Stress";
}
