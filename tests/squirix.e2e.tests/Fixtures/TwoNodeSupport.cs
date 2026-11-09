namespace Squirix.E2ETests.Fixtures;

/// <summary>Shared startup, routing, and assertion helpers for two-node public API e2e tests.</summary>
internal static class TwoNodeSupport
{
    internal static string FindKeyOwnedBy(string cacheName, string ownerId, string prefix) => KeyOwnerHelper.TwoNode.FindKeyOwnedBy(cacheName, ownerId, prefix);
}
