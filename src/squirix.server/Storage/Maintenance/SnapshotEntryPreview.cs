namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes one snapshot entry preview.
/// </summary>
/// <param name="Key">The namespaced cache key.</param>
/// <param name="Version">The entry version.</param>
[JetBrains.Annotations.UsedImplicitly(JetBrains.Annotations.ImplicitUseTargetFlags.WithMembers)]
internal readonly record struct SnapshotEntryPreview(string Key, long Version);
