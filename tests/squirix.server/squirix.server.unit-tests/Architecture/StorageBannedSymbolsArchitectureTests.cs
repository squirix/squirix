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
/// Enforces the storage-only bans for <c language="csharp">src/squirix.server/Storage</c>; Roslyn banned-API lists apply to a whole project
/// and cannot be scoped to a folder, and <c language="csharp">Squirix.Server</c> legitimately parses JSON and returns pooled buffers elsewhere.
/// </summary>
/// <remarks>
/// This is a textual scan without a semantic model: each file is masked by <see cref="CsharpSourceMasker"/> (comments and
/// literal text become spaces) and the banned patterns run over the whole masked text, so calls split across lines are
/// still found; interpolation holes stay visible as code. Known limits: a pool held under a name without "pool" is not
/// recognized, aliases (<see langword="using"/> aliases, locals holding <c language="csharp">Encoding.UTF8</c>) are not
/// followed, a top-level comma inside brackets or generic arguments reads as a second argument, and inactive
/// <c language="csharp">#if</c> code is scanned as text.
/// </remarks>
[Immutable]
public sealed class StorageBannedSymbolsArchitectureTests : ServerUnitTestBase
{
    private const string SanctionedReturnHelper = "ArrayPoolExtensions.cs";

    private const RegexOptions PatternOptions = RegexOptions.CultureInvariant;

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(1);

    private static readonly (string Name, Regex Pattern, string Reason)[] BannedPatterns =
    [
        ("JsonElement.GetRawText", new Regex(@"\.\s*GetRawText\s*\(", PatternOptions, MatchTimeout), "avoid UTF-8 JSON text round trips on persistence paths"),
        ("JsonSerializer.SerializeToUtf8Bytes", new Regex(@"\bSerializeToUtf8Bytes\s*[<(]", PatternOptions, MatchTimeout), "use binary codecs on disk, not UTF-8 JSON blobs"),
        ("JsonDocument.Parse", new Regex(@"\bJsonDocument\s*\.\s*Parse\s*\(", PatternOptions, MatchTimeout), "use binary codecs on persistence paths instead of JSON parse"),
        ("Encoding.UTF8.GetBytes(string)", new Regex(@"\bEncoding\s*\.\s*UTF8\s*\.\s*GetBytes\s*\((?>[^(),]+|(?<depth>\()|(?<-depth>\))|(?(depth),|(?!)))*(?(depth)(?!))\)", PatternOptions, MatchTimeout), "use GetByteCount plus GetBytes(string, span), or encode into a pre-sized buffer"),
        ("ArrayPool.Return", new Regex(@"(?:\bArrayPool\s*<[^>]+>\s*\.\s*Shared|\b\w*(?i:pool)\w*)\s*\.\s*Return\s*\(", PatternOptions, MatchTimeout), "pooled buffers carry operation data; return them through ArrayPoolExtensions.ReturnCleared so they are cleared before reuse"),
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

            FindViolations(path, await File.ReadAllTextAsync(path, cancellationToken), violations);
        }

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Ensures the scanner reports every banned symbol shape.</summary>
    [Test]
    public async Task ScannerDetectsBannedSymbols()
    {
        const string source =
            "var a = element.GetRawText();\n" +
            "var b = JsonSerializer.SerializeToUtf8Bytes(value, options);\n" +
            "using var c = JsonDocument.Parse(memory);\n" +
            "var d = Encoding.UTF8.GetBytes(text);\n" +
            "ArrayPool<byte>.Shared.Return(buffer);\n" +
            "pool.Return(buffer, true);\n";

        var violations = new List<string>();
        FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations.Count).IsEqualTo(6);
    }

    /// <summary>Ensures the scanner accepts sanctioned shapes and ignores comments.</summary>
    [Test]
    public async Task ScannerAcceptsAllowedShapes()
    {
        const string source =
            "_ = Encoding.UTF8.GetBytes(text, destination[2..]);\n" +
            "offset += Encoding.UTF8.GetBytes(key, destination[offset..]);\n" +
            "ArrayPool<byte>.Shared.ReturnCleared(buffer);\n" +
            "// ArrayPool<byte>.Shared.Return(buffer);\n" +
            "/// <see cref=\"JsonElement.GetRawText()\" />\n";

        var violations = new List<string>();
        FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations).IsEmpty();
    }

    /// <summary>Ensures banned calls split across lines are reported on the line where the match starts.</summary>
    /// <param name="source">The fixture source.</param>
    /// <param name="line">The expected 1-based line.</param>
    [Test]
    [Arguments("var x = 1;\nvar d = Encoding.UTF8\n    .GetBytes(text);", 2)]
    [Arguments("var x = 1;\r\nvar d = Encoding\r\n    .UTF8\r\n    .GetBytes(\r\n        text);", 2)]
    [Arguments("var d = Encoding.UTF8.GetBytes(\n    Format(a, b));", 1)]
    [Arguments("var x = 1;\n_pool\n    .Return(buffer);", 2)]
    [Arguments("var x = 1;\nthis.bufferPool\n    .Return(buffer, true);", 2)]
    [Arguments("var x = 1;\nArrayPool<byte>.Shared\n    .Return(buffer);", 2)]
    [Arguments("var a = element\n    .GetRawText();", 2)]
    [Arguments("using var c = JsonDocument\n    .Parse(m);", 1)]
    public async Task ScannerDetectsMultilineCalls(string source, int line)
    {
        var violations = new List<string>();
        FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations.Count).IsEqualTo(1);
        _ = await Assert.That(violations[0]).StartsWith($"fixture.cs({line}): ");
    }

    /// <summary>Ensures banned names inside comments and literals, span overloads and clearing returns are not reported.</summary>
    /// <param name="source">The fixture source.</param>
    [Test]
    [Arguments("/* element.GetRawText(); */")]
    [Arguments("/*\n * JsonDocument.Parse(m);\n */")]
    [Arguments("var a = 1; // JsonSerializer.SerializeToUtf8Bytes(value)")]
    [Arguments("/// <see cref=\"JsonElement.GetRawText()\" />")]
    [Arguments("var s = \"element.GetRawText()\";")]
    [Arguments("var s = \"a\\\"JsonDocument.Parse(m)\";")]
    [Arguments("var s = @\"a\"\"b\nJsonDocument.Parse(m)\";")]
    [Arguments("var s = $\"{name}: JsonDocument.Parse(m)\";")]
    [Arguments("var s = $@\"{name}\nelement.GetRawText()\";")]
    [Arguments("var s = @$\"{name} element.GetRawText()\";")]
    [Arguments("var s = $\"{value:N2} JsonDocument.Parse(m)\";")]
    [Arguments("var s = \"\"\"\n    \"JsonDocument.Parse(m)\"\n    \"\"\";")]
    [Arguments("var s = $\"\"\"\n    {name} SerializeToUtf8Bytes(v)\n    \"\"\";")]
    [Arguments("var s = \"\"\"\" \"\"\" element.GetRawText() \"\"\"\";")]
    [Arguments("var s = \"_pool.Return(buffer)\";")]
    [Arguments("_ = Encoding.UTF8.GetBytes(text, destination);")]
    [Arguments("_ = Encoding.UTF8\n    .GetBytes(\n        text,\n        destination[offset..]);")]
    [Arguments("_ = Encoding.UTF8.GetBytes(Format(a), destination);")]
    [Arguments("_ = Encoding.UTF8.GetBytes(text /* one ) */, destination);")]
    [Arguments("_pool\n    .ReturnCleared(buffer);")]
    [Arguments("ArrayPool<byte>.Shared\n    .ReturnCleared(buffer);")]
    public async Task ScannerAcceptsMaskedAndAllowedCode(string source)
    {
        var violations = new List<string>();
        FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations).IsEmpty();
    }

    /// <summary>Ensures a banned call after a literal or comment holding quote or comment characters is reported.</summary>
    /// <param name="source">The fixture source.</param>
    [Test]
    [Arguments("var s = \"// not a comment\"; x.GetRawText();")]
    [Arguments("/* c */ JsonDocument.Parse(m);")]
    [Arguments("var q = '\"'; x.GetRawText();")]
    [Arguments("var q = '\\''; x.GetRawText();")]
    [Arguments("var q = '\\\\'; x.GetRawText();")]
    [Arguments("var s = @\"a\\\"; x.GetRawText();")]
    [Arguments("var s = \"\"; x.GetRawText();")]
    [Arguments("var s = $\"{JsonDocument.Parse(m)}\";")]
    [Arguments("var s = $\"\"\"{Encoding.UTF8.GetBytes(text)}\"\"\";")]
    [Arguments("#if DEBUG // \"\nx.GetRawText();\n#endif")]
    public async Task ScannerDetectsCallsAfterLiterals(string source)
    {
        var violations = new List<string>();
        FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations.Count).IsEqualTo(1);
    }

    private static void FindViolations(string path, string source, List<string> violations)
    {
        var masked = CsharpSourceMasker.Mask(source);
        for (var patternIndex = 0; patternIndex < BannedPatterns.Length; patternIndex++)
        {
            var (name, pattern, reason) = BannedPatterns[patternIndex];
            for (var match = pattern.Match(masked); match.Success; match = match.NextMatch())
            {
                var line = masked.AsSpan(0, match.Index).Count('\n') + 1;
                violations.Add($"{path}({line}): {name} is banned under Storage: {reason}.");
            }
        }
    }
}
