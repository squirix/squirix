using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.SourceScanning;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Ensures every server unit-test area is covered by an explicit dependency policy.</summary>
[Immutable]
public sealed partial class TestAreaPolicyTests : ServerUnitTestBase
{
    [GeneratedRegex(@"^\s*namespace\s+(?<namespace>Squirix\.Server\.UnitTests(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*[;{]", RegexOptions.Multiline | RegexOptions.CultureInvariant, 1000)]
    private static partial Regex NamespacePattern { get; }

    /// <summary>Unknown areas and names resembling approved areas are rejected by the fallback policy.</summary>
    /// <param name="testNamespace">The namespace without an explicit area policy.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("Squirix.Server.UnitTests.Unmapped")]
    [Arguments("Squirix.Server.UnitTests.Unmapped.Nested")]
    [Arguments("Squirix.Server.UnitTests.PersistenceExtra")]
    public async Task UnknownAreasAreRejected(string? testNamespace, CancellationToken cancellationToken)
    {
        var value = await Assert.That(testNamespace).IsNotNull();
        var policy = await LoadPolicyAsync(cancellationToken);
        var guard = await Assert.That(policy.SelectSingleNode("/NsDepCopConfig/Disallowed[@To='*']") as XmlElement).IsNotNull();
        _ = await Assert.That(Matches(guard.GetAttribute("From"), value)).IsTrue();
    }

    /// <summary>Each declared test namespace belongs to a registered area with a specific dependency restriction.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeclaredAreasHavePolicies(CancellationToken cancellationToken)
    {
        var policy = await LoadPolicyAsync(cancellationToken);
        var guard = await Assert.That(policy.SelectSingleNode("/NsDepCopConfig/Disallowed[@To='*']") as XmlElement).IsNotNull();
        var domains = new List<string>();
        var rules = await Assert.That(policy.SelectNodes("/NsDepCopConfig/Disallowed")).IsNotNull();
        foreach (XmlNode node in rules)
        {
            if (node is XmlElement rule && !ReferenceEquals(rule, guard))
                domains.Add(rule.GetAttribute("From"));
        }

        var root = Path.Join(RepositoryPaths.FindRepositoryRoot(), "tests", "squirix.server", "squirix.server.unit-tests");
        var checkedNamespaces = new HashSet<string>(StringComparer.Ordinal);
        foreach (var path in Directory.GetFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal) ||
                path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                continue;

            var source = CsharpSourceMasker.Mask(await File.ReadAllTextAsync(path, cancellationToken));
            foreach (Match match in NamespacePattern.Matches(source))
            {
                var testNamespace = match.Groups["namespace"].Value;
                if (testNamespace.Equals("Squirix.Server.UnitTests", StringComparison.Ordinal) || !checkedNamespaces.Add(testNamespace))
                    continue;

                _ = await Assert.That(Matches(guard.GetAttribute("From"), testNamespace)).IsFalse().Because(path);
                _ = await Assert.That(HasSpecificPolicy(domains, testNamespace)).IsTrue().Because(path);
            }
        }

        _ = await Assert.That(checkedNamespaces.Count).IsGreaterThan(0);
    }

    private static bool HasSpecificPolicy(List<string> domains, string testNamespace)
    {
        for (var index = 0; index < domains.Count; index++)
        {
            if (Matches(domains[index], testNamespace) && !Matches(domains[index], "Squirix.Server.UnitTests.Unmapped"))
                return true;
        }

        return false;
    }

    private static async Task<XmlDocument> LoadPolicyAsync(CancellationToken cancellationToken)
    {
        var path = Path.Join(RepositoryPaths.FindRepositoryRoot(), "tests", "squirix.server", "squirix.server.unit-tests", "config.nsdepcop");
        var policy = new XmlDocument();
        policy.LoadXml(await File.ReadAllTextAsync(path, cancellationToken));
        return policy;
    }

    private static bool Matches(string domain, string testNamespace)
    {
        return domain switch
        {
            _ when domain.StartsWith('/') => Regex.IsMatch(testNamespace, domain[1..^1], RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1)),
            _ when domain.EndsWith(".*", StringComparison.Ordinal) => testNamespace.Equals(domain[..^2], StringComparison.Ordinal) || testNamespace.StartsWith(domain[..^1], StringComparison.Ordinal),
            _ => domain.Equals("*", StringComparison.Ordinal) || testNamespace.Equals(domain, StringComparison.Ordinal),
        };
    }
}
