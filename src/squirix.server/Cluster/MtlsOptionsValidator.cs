using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
[Immutable]
internal sealed class MtlsOptionsValidator : IValidateOptions<MtlsOptions>
{
    private readonly TopologyOptions _cluster;

    internal MtlsOptionsValidator(TopologyOptions cluster)
    {
        _cluster = cluster;
    }

    public ValidateOptionsResult Validate(string? name, MtlsOptions options)
    {
        try
        {
            var primaryListenPort = _cluster.Uri.IsAbsoluteUri ? _cluster.Uri.Port : default(int?);
            options.Validate(primaryListenPort, MtlsTopology.RequiresInterNodeMtls(_cluster));
            return ValidateOptionsResult.Success;
        }
        catch (InvalidOperationException ex)
        {
            return ValidateOptionsResult.Fail(ex.Message);
        }
    }
}
