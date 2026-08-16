using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Squirix.Attributes;

namespace Squirix.Server.Cluster;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
[Immutable]
internal sealed class ConfigValidator : IValidateOptions<TopologyOptions>
{
    public ValidateOptionsResult Validate(string? name, TopologyOptions options) =>
        TopologyValidator.TryValidate(options, out var failures) ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
}
