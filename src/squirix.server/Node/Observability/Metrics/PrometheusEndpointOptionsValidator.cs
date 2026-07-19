using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.Observability.Metrics;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
internal sealed class PrometheusEndpointOptionsValidator : IValidateOptions<PrometheusMetricsEndpointOptions>
{
    public ValidateOptionsResult Validate(string? name, PrometheusMetricsEndpointOptions options) => options switch
    {
        { Enabled: false } => ValidateOptionsResult.Success,
        _ when string.IsNullOrWhiteSpace(options.Path) => ValidateOptionsResult.Fail("Prometheus metrics Path must be non-empty when the endpoint is enabled."),
        _ when !options.Path.StartsWith('/') => ValidateOptionsResult.Fail("Prometheus metrics Path must start with '/'."),
        _ => ValidateOptionsResult.Success,
    };
}
