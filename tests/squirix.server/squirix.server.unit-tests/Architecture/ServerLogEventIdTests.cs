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

/// <summary>Every source-generated log message of Squirix.Server carries its own event id, so an id identifies exactly one message.</summary>
[Immutable]
public sealed class ServerLogEventIdTests
{
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(1);

    private static readonly Regex MessageAttributePattern = new(@"\[\s*LoggerMessage\s*\((?<arguments>[^)]*)\)\s*\]\s*(?:internal|public|private|protected)?\s*(?:static\s+)?partial\s+void\s+(?<name>\w+)", RegexOptions.CultureInvariant, MatchTimeout);

    private static readonly Regex EventIdPattern = new(@"\bEventId\s*=\s*(?<id>\d+)\b", RegexOptions.CultureInvariant, MatchTimeout);

    /// <summary>Ensures no two log messages share an event id and every message declares a literal one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EventIdsShouldBeUnique(CancellationToken cancellationToken)
    {
        var files = await ServerSourceFiles.EnumerateCsharpFilesAsync();
        var owners = new Dictionary<int, string>();
        var violations = new List<string>();
        var messageCount = 0;
        for (var index = 0; index < files.Count; index++)
        {
            var path = files[index];
            var masked = CsharpSourceMasker.Mask(await File.ReadAllTextAsync(path, cancellationToken));
            for (var match = MessageAttributePattern.Match(masked); match.Success; match = match.NextMatch())
            {
                messageCount++;
                var name = match.Groups["name"].Value;
                var eventId = EventIdPattern.Match(match.Groups["arguments"].Value);
                if (!eventId.Success)
                {
                    violations.Add($"{Path.GetFileName(path)}: {name} does not declare a literal EventId.");
                    continue;
                }

                var id = int.Parse(eventId.Groups["id"].Value, CultureInfo.InvariantCulture);
                if (!owners.TryAdd(id, name))
                    violations.Add($"{Path.GetFileName(path)}: {name} reuses event id {id} of {owners[id]}.");
            }
        }

        _ = await Assert.That(messageCount).IsGreaterThan(0);
        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }
}
