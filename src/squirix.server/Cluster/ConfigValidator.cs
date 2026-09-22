using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

[Immutable]
internal sealed class ConfigValidator : IValidateOptions<TopologyOptions>
{
    public ValidateOptionsResult Validate(string? name, TopologyOptions options) =>
        TopologyValidator.TryValidate(options, out var failures) ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
}
