using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability.Metrics;

[Immutable]
internal sealed class PrometheusEndpointOptionsValidator : IValidateOptions<PrometheusMetricsEndpointOptions>
{
    public ValidateOptionsResult Validate(string? name, PrometheusMetricsEndpointOptions options)
    {
        var failure = ValidatePath(options.Enabled, options.Path);
        return failure == null ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failure);
    }

    internal static string? ValidatePath(bool enabled, string? path) => enabled switch
    {
        false => null,
        _ when string.IsNullOrWhiteSpace(path) => "Prometheus metrics Path must be non-empty when the endpoint is enabled.",
        _ when path[0] != '/' => "Prometheus metrics Path must start with '/'.",
        _ => null,
    };
}
