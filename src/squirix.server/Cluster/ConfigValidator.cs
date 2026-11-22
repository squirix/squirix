using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster;

[Immutable]
internal sealed class ConfigValidator : ISettingsRule<TopologyOptions>
{
    public IReadOnlyList<string> Check(TopologyOptions options) =>
        TopologyValidator.TryValidate(options, out var failures) ? [] : failures;
}
