using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.SourceScanning;
using Squirix.Server.UnitTests.Support;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules that keep loggers explicit: they arrive only through required constructor or method arguments.</summary>
[Immutable]
public sealed class LoggerArchitectureTests : ServerUnitTestBase
{
    private static readonly string AssemblyPath = Path.Join(AppContext.BaseDirectory, "Squirix.Server.dll");

    private static readonly string[] ProductionSourceRoots =
    [
        Path.Join("src", "squirix"),
        Path.Join("src", "squirix.server"),
        Path.Join("src", "squirix.server.host"),
        Path.Join("src", "shared"),
    ];

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(1);

    private static readonly Regex NullLoggerPattern = new(@"\bNullLogger(?:Factory|Provider)?\b|\bLoggerFactory\s*\.\s*Create\s*\(|\bnew\s+LoggerFactory\s*\(", RegexOptions.CultureInvariant, MatchTimeout);

    /// <summary>Ensures no static field, property or method holds or returns a logger.</summary>
    [Test]
    public async Task StaticMembersShouldNotHoldLoggers()
    {
        var violations = LoggerArchitectureScanner.FindStaticLoggerMembers(AssemblyPath);

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Ensures static classes take no logger parameters apart from source-generated logging methods.</summary>
    [Test]
    public async Task StaticClassesShouldNotTakeLoggers()
    {
        var violations = LoggerArchitectureScanner.FindStaticClassLoggerParameters(AssemblyPath);

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Ensures logger parameters, fields and properties are required and immutable.</summary>
    [Test]
    public async Task LoggersShouldBeRequired()
    {
        var violations = LoggerArchitectureScanner.FindOptionalLoggers(AssemblyPath);

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Ensures production sources never create a null logger or a logger factory of their own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionShouldNotCreateFallbackLoggers(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var violations = new List<string>();
        for (var rootIndex = 0; rootIndex < ProductionSourceRoots.Length; rootIndex++)
        {
            var directory = Path.Join(root, ProductionSourceRoots[rootIndex]);
            _ = await Assert.That(Directory.Exists(directory)).IsTrue().Because($"Expected source root '{directory}'.");

            var files = Directory.GetFiles(directory, "*.cs", SearchOption.AllDirectories);
            for (var index = 0; index < files.Length; index++)
            {
                var path = files[index];
                if (IsBuildOutput(path))
                    continue;

                var masked = CsharpSourceMasker.Mask(await File.ReadAllTextAsync(path, cancellationToken));
                for (var match = NullLoggerPattern.Match(masked); match.Success; match = match.NextMatch())
                {
                    var line = masked.AsSpan(0, match.Index).Count('\n') + 1;
                    violations.Add($"{path}({line}): '{match.Value}' is banned in production code; take the logger as a required argument.");
                }
            }
        }

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    private static bool IsBuildOutput(string path)
    {
        var separator = Path.DirectorySeparatorChar;
        return path.Contains($"{separator}obj{separator}", StringComparison.Ordinal) || path.Contains($"{separator}bin{separator}", StringComparison.Ordinal);
    }
}
