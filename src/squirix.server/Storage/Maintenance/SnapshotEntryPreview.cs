using JetBrains.Annotations;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes one snapshot entry preview.
/// </summary>
/// <param name="Key">The namespaced cache key.</param>
/// <param name="Version">The entry version.</param>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal readonly record struct SnapshotEntryPreview(string Key, long Version);
