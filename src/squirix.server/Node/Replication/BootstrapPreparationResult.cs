using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Replication;

/// <summary>Outcome of stopped-cluster bootstrap preparation.</summary>
/// <param name="Manifest">Created or resumed manifest.</param>
/// <param name="Resumed">Whether an identical existing manifest was resumed.</param>
/// <param name="ManifestPath">Published manifest path.</param>
[Immutable]
internal sealed record BootstrapPreparationResult(BootstrapManifest Manifest, bool Resumed, string ManifestPath);
