using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Squirix.Server.UnitTests.SourceScanning;

/// <summary>
/// Scans C# sources for the storage-only bans under <c language="csharp">src/squirix.server/Storage</c>; Roslyn banned-API lists apply to a
/// whole project and cannot be scoped to a folder, and <c language="csharp">Squirix.Server</c> legitimately parses JSON and returns pooled
/// buffers elsewhere.
/// </summary>
/// <remarks>
/// This is a textual scan without a semantic model: each file is masked by <see cref="CsharpSourceMasker"/> (comments and
/// literal text become spaces) and the banned patterns run over the whole masked text, so calls split across lines are
/// still found; interpolation holes stay visible as code. Known limits: a pool held under a name without "pool" is not
/// recognized, aliases (<see langword="using"/> aliases, locals holding <c language="csharp">Encoding.UTF8</c>) are not
/// followed, a top-level comma inside brackets or generic arguments reads as a second argument, and inactive
/// <c language="csharp">#if</c> code is scanned as text.
/// </remarks>
internal static class StorageBanScanner
{
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

    /// <summary>Masks the source and appends one violation per banned match.</summary>
    /// <param name="path">The path reported in each violation.</param>
    /// <param name="source">The C# source text.</param>
    /// <param name="violations">The list that receives the violations.</param>
    internal static void FindViolations(string path, string source, List<string> violations)
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
