using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Storage;

namespace Squirix.Server.Node.Replication;

/// <summary>Validated source and target inputs for stopped-cluster bootstrap preparation.</summary>
[Immutable]
internal sealed class BootstrapPreparationRequest
{
    /// <summary>Gets deterministic replica-group identities derived from RF=1 owners.</summary>
    internal required IReadOnlyList<string> GroupIds { get; init; }

    /// <summary>Gets validated legacy outcomes discovered in source persistence.</summary>
    internal required IReadOnlyList<BootstrapLegacyOutcome> LegacyOutcomes { get; init; }

    /// <summary>Gets persistence configuration; null means persistence is disabled.</summary>
    internal PersistenceOptions? Persistence { get; init; }

    /// <summary>Gets source inter-node address derivation settings.</summary>
    internal required MtlsOptions SourceMtls { get; init; }

    /// <summary>Gets validated RF=1 source topology.</summary>
    internal required TopologyOptions SourceTopology { get; init; }

    /// <summary>Gets target inter-node address derivation settings.</summary>
    internal required MtlsOptions TargetMtls { get; init; }

    /// <summary>Gets requested RF&gt;1 target topology.</summary>
    internal required TopologyOptions TargetTopology { get; init; }
}
