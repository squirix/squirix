using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.SourceScanning;

/// <summary>Covers <see cref="StorageBanScanner"/>, the textual scanner behind the storage banned-symbols architecture check.</summary>
[Immutable]
public sealed class StorageBanScannerTests : ServerUnitTestBase
{
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
        StorageBanScanner.FindViolations("fixture.cs", source, violations);

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
        StorageBanScanner.FindViolations("fixture.cs", source, violations);

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
        StorageBanScanner.FindViolations("fixture.cs", source, violations);

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
        StorageBanScanner.FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations).IsEmpty();
    }

    /// <summary>Ensures a banned call after a literal or comment holding quote or comment characters is reported.</summary>
    /// <param name="source">The fixture source.</param>
    [Test]
    [Arguments("var s = \"// not a comment\"; x.GetRawText();")]
    [Arguments("/* c */ JsonDocument.Parse(m);")]
    [Arguments("var q = '\"'; x.GetRawText();")]
    [Arguments("var q = '\\''; x.GetRawText();")]
    [Arguments(@"var q = '\\'; x.GetRawText();")]
    [Arguments("var s = @\"a\\\"; x.GetRawText();")]
    [Arguments("var s = \"\"; x.GetRawText();")]
    [Arguments("var s = $\"{JsonDocument.Parse(m)}\";")]
    [Arguments("var s = $\"\"\"{Encoding.UTF8.GetBytes(text)}\"\"\";")]
    [Arguments("#if DEBUG // \"\nx.GetRawText();\n#endif")]
    public async Task ScannerDetectsCallsAfterLiterals(string source)
    {
        var violations = new List<string>();
        StorageBanScanner.FindViolations("fixture.cs", source, violations);

        _ = await Assert.That(violations.Count).IsEqualTo(1);
    }
}
