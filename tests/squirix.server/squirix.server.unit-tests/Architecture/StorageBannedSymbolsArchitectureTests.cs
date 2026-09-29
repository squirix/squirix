using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Enforces the storage-only bans documented in <c language="csharp">BannedSymbols.Storage.txt</c> for
/// <c language="csharp">src/squirix.server/Storage</c>; Roslyn banned-API lists apply to a whole project and cannot be scoped to a folder.
/// </summary>
[Immutable]
public sealed class StorageBannedSymbolsArchitectureTests : ServerUnitTestBase
{
    private const string SanctionedReturnHelper = "ArrayPoolExtensions.cs";

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(1);

    private static readonly (string Name, Regex Pattern)[] BannedPatterns =
    [
        ("JsonElement.GetRawText", new Regex(@"\.GetRawText\s*\(", RegexOptions.CultureInvariant, MatchTimeout)),
        ("JsonSerializer.SerializeToUtf8Bytes", new Regex(@"\bSerializeToUtf8Bytes\s*[<(]", RegexOptions.CultureInvariant, MatchTimeout)),
        ("JsonDocument.Parse", new Regex(@"\bJsonDocument\.Parse\s*\(", RegexOptions.CultureInvariant, MatchTimeout)),
        ("Encoding.UTF8.GetBytes(string)", new Regex(@"\bEncoding\.UTF8\.GetBytes\s*\((?:[^(),]|\([^()]*\))*\)", RegexOptions.CultureInvariant, MatchTimeout)),
        ("ArrayPool.Return", new Regex(@"\b(?:ArrayPool<[^>]+>\.Shared|\w*[Pp]ool\w*)\.Return\s*\(", RegexOptions.CultureInvariant, MatchTimeout)),
    ];

    /// <summary>Ensures persistence sources do not use the symbols banned for storage code.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StorageSourcesDoNotUseBannedSymbols(CancellationToken cancellationToken)
    {
        var files = await ServerSourceFiles.EnumerateCsharpFilesAsync("Storage");
        var violations = new List<string>();
        for (var index = 0; index < files.Count; index++)
        {
            var path = files[index];
            if (string.Equals(Path.GetFileName(path), SanctionedReturnHelper, StringComparison.Ordinal))
                continue;

            FindViolations(path, await File.ReadAllLinesAsync(path, cancellationToken), violations);
        }

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Ensures the scanner reports every banned symbol shape.</summary>
    [Test]
    public async Task ScannerDetectsBannedSymbols()
    {
        string[] source =
        [
            "var a = element.GetRawText();",
            "var b = JsonSerializer.SerializeToUtf8Bytes(value, options);",
            "using var c = JsonDocument.Parse(memory);",
            "var d = Encoding.UTF8.GetBytes(text);",
            "ArrayPool<byte>.Shared.Return(buffer);",
            "pool.Return(buffer, true);",
        ];

        var violations = new List<string>();
        FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations.Count).IsEqualTo(6);
    }

    /// <summary>Ensures the scanner accepts sanctioned shapes and ignores comments.</summary>
    [Test]
    public async Task ScannerAcceptsAllowedShapes()
    {
        string[] source =
        [
            "_ = Encoding.UTF8.GetBytes(text, destination[2..]);",
            "offset += Encoding.UTF8.GetBytes(key, destination[offset..]);",
            "ArrayPool<byte>.Shared.ReturnCleared(buffer);",
            "// ArrayPool<byte>.Shared.Return(buffer);",
            "/// <see cref=\"JsonElement.GetRawText()\" />",
        ];

        var violations = new List<string>();
        FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations).IsEmpty();
    }

    private static void FindViolations(string path, string[] lines, List<string> violations)
    {
        for (var lineIndex = 0; lineIndex < lines.Length; lineIndex++)
        {
            var line = lines[lineIndex];
            if (line.AsSpan().TrimStart().StartsWith("//", StringComparison.Ordinal))
                continue;

            for (var patternIndex = 0; patternIndex < BannedPatterns.Length; patternIndex++)
            {
                var (name, pattern) = BannedPatterns[patternIndex];
                if (pattern.IsMatch(line))
                    violations.Add($"{path}({lineIndex + 1}): {name} is banned under Storage; see BannedSymbols.Storage.txt.");
            }
        }
    }
}
