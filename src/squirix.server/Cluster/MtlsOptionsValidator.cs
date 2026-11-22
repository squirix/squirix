using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster;

[Immutable]
internal sealed class MtlsOptionsValidator : ISettingsRule<MtlsOptions>
{
    private readonly TopologyOptions _cluster;

    internal MtlsOptionsValidator(TopologyOptions cluster)
    {
        _cluster = cluster;
    }

    public IReadOnlyList<string> Check(MtlsOptions options)
    {
        try
        {
            var primaryListenPort = _cluster.Uri.IsAbsoluteUri ? _cluster.Uri.Port : default(int?);
            options.Validate(primaryListenPort, MtlsTopology.RequiresInterNodeMtls(_cluster));
            return [];
        }
        catch (InvalidOperationException ex)
        {
            return [ex.Message];
        }
    }
}
