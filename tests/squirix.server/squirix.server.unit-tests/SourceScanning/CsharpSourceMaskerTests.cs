using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.SourceScanning;

/// <summary>Covers the comment and literal masker used by textual architecture scans.</summary>
[Immutable]
public sealed class CsharpSourceMaskerTests : ServerUnitTestBase
{
    /// <summary>Ensures masking keeps the length and every line break of the source.</summary>
    [Test]
    public async Task MaskPreservesLengthAndLineBreaks()
    {
        const string source = "var a = \"x\"; // c\r\n/* b\r\n */ var s = @\"q\r\n\"\"r\";\r\nvar t = \"\"\"\r\n  raw\r\n  \"\"\";\r\n";

        var masked = CsharpSourceMasker.Mask(source);

        var mismatches = 0;
        for (var index = 0; index < source.Length; index++)
        {
            var isLineBreak = source[index] is '\r' or '\n';
            if (isLineBreak != (masked[index] is '\r' or '\n'))
                mismatches++;
        }

        _ = await Assert.That(masked.Length).IsEqualTo(source.Length);
        _ = await Assert.That(mismatches).IsEqualTo(0);
    }

    /// <summary>Ensures comments and literal text are blanked while code, holes and directives stay.</summary>
    /// <param name="source">The source text.</param>
    /// <param name="expected">The expected masked text.</param>
    [Test]
    [Arguments("a /* b */ c", "a         c")]
    [Arguments("a; // b", "a;     ")]
    [Arguments("/// <c>x</c>\ny", "            \ny")]
    [Arguments("x = \"a\\\"b\";", "x =       ;")]
    [Arguments("x = @\"a\"\"b\";", "x =        ;")]
    [Arguments("x = @\"a\\\"; y", "x =      ; y")]
    [Arguments("x = '\"'; y", "x =    ; y")]
    [Arguments("x = '\\''; y", "x =     ; y")]
    [Arguments(@"x = '\\'; y", "x =     ; y")]
    [Arguments("$\"a{b}c\"", "    b   ")]
    [Arguments("$\"{v:N2}\"", "   v     ")]
    [Arguments("$\"{f(\"s\")}\"", "   f(   )  ")]
    [Arguments("$\"{global::A.B}\"", "   global::A.B  ")]
    [Arguments("$@\"{a}\n\"", "    a \n ")]
    [Arguments("@$\"{{a}}\"", "         ")]
    [Arguments("\"\"\"\" \"\"\" a \"\"\"\"; b", "               ; b")]
    [Arguments("$$\"\"\"{a}{{b}}\"\"\"", "          b     ")]
    [Arguments("#if DEBUG // x\ny", "#if DEBUG // x\ny")]
    public async Task MaskProducesExpectedText(string source, string expected) =>
        _ = await Assert.That(CsharpSourceMasker.Mask(source)).IsEqualTo(expected);

    /// <summary>Ensures unterminated comments and literals are masked to the end without throwing.</summary>
    /// <param name="source">The source text.</param>
    /// <param name="expected">The expected masked text.</param>
    [Test]
    [Arguments("a /* b\nc", "a     \n ")]
    [Arguments("x = \"abc\ny", "x =     \n ")]
    [Arguments("x = @\"a", "x =    ")]
    [Arguments("x = \"\"\"\nabc", "x =    \n   ")]
    [Arguments("x = $\"{a", "x =    a")]
    [Arguments("x = \"\\", "x =   ")]
    [Arguments("'\\", "  ")]
    public async Task MaskHandlesUnterminatedConstructs(string source, string expected) =>
        _ = await Assert.That(CsharpSourceMasker.Mask(source)).IsEqualTo(expected);
}
