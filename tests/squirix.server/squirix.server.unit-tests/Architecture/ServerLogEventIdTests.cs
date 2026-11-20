using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.SourceScanning;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Every declared node event must have its own explicit numeric identifier.</summary>
[Immutable]
public sealed partial class ServerLogEventIdTests
{
    [GeneratedRegex(@"\bstatic\s+readonly\s+LogEvent\s+(?<name>\w+)\s*=\s*(?:new\s*(?:LogEvent\s*)?\(\s*(?<id>[0-9]+)\s*,)?", RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture | RegexOptions.NonBacktracking)]
    private static partial Regex DescriptorPattern { get; }

    /// <summary>Checks all event descriptors, including those on private component types.</summary>
    /// <param name="cancellationToken">Test cancellation.</param>
    [Test]
    public async Task EventIdsShouldBeUnique(CancellationToken cancellationToken)
    {
        var owners = new Dictionary<int, string>();
        var violations = new List<string>();
        foreach (var path in await ServerSourceFiles.EnumerateCsharpFilesAsync())
        {
            var source = await File.ReadAllTextAsync(path, cancellationToken);
            if (!source.Contains("LogEvent", StringComparison.Ordinal))
                continue;

            foreach (Match match in DescriptorPattern.Matches(CsharpSourceMasker.Mask(source)))
            {
                var owner = $"{Path.GetFileName(path)}.{match.Groups["name"].Value}";
                if (!int.TryParse(match.Groups["id"].ValueSpan, NumberStyles.None, CultureInfo.InvariantCulture, out var id))
                    violations.Add($"{owner} has no explicit numeric event id.");
                else if (!owners.TryAdd(id, owner))
                    violations.Add($"{owner} reuses event id {id} of {owners[id]}.");
            }
        }

        _ = await Assert.That(owners.Count).IsGreaterThan(0);
        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }
}
