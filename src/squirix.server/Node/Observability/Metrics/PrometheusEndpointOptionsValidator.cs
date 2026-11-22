using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Observability.Metrics;

[Immutable]
internal sealed class PrometheusEndpointOptionsValidator : ISettingsRule<PrometheusMetricsEndpointOptions>
{
    public IReadOnlyList<string> Check(PrometheusMetricsEndpointOptions options)
    {
        var failure = ValidatePath(options.Enabled, options.Path);
        return failure == null ? [] : [failure];
    }

    internal static string? ValidatePath(bool enabled, string? path) => enabled switch
    {
        false => null,
        _ when string.IsNullOrWhiteSpace(path) => "Prometheus metrics Path must be non-empty when the endpoint is enabled.",
        _ when path[0] != '/' => "Prometheus metrics Path must start with '/'.",
        _ => null,
    };
}
