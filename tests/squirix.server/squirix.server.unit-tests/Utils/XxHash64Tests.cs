using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Reference vectors for the in-repo XXH64 implementation with seed 0.</summary>
[Immutable]
public sealed class XxHash64Tests : ServerUnitTestBase
{
    private const int PatternMultiplier = 31;
    private const int PatternOffset = 7;

    /// <summary>Text inputs match the published XXH64 values.</summary>
    /// <param name="text">The input text, hashed as UTF-8.</param>
    /// <param name="expected">The reference hash.</param>
    [Test]
    [Arguments("", 0xEF46DB3751D8E999UL)]
    [Arguments("a", 0xD24EC4F1A98C6E5BUL)]
    [Arguments("abc", 0x44BC2CF5AD770999UL)]
    [Arguments("hello world", 0x45AB6734B21E6968UL)]
    public async Task TextMatchesReference(string text, ulong expected)
    {
        var actual = XxHash64.Hash(System.Text.Encoding.UTF8.GetBytes(text));
        _ = await Assert.That(actual).IsEqualTo(expected);
    }

    /// <summary>Patterned inputs of every tail and stripe shape match an independent reference implementation.</summary>
    /// <param name="length">The input length in bytes.</param>
    /// <param name="expected">The reference hash.</param>
    [Test]
    [Arguments(0, 0xEF46DB3751D8E999UL)]
    [Arguments(1, 0xA96C7F0CE858BBB7UL)]
    [Arguments(2, 0xAC378C5993CD5F9AUL)]
    [Arguments(3, 0x56E6957632A487F9UL)]
    [Arguments(4, 0xC60D15B1E3FF8F04UL)]
    [Arguments(5, 0x808815858624DD4EUL)]
    [Arguments(7, 0xAFBEFC3D6C6F9A8EUL)]
    [Arguments(8, 0x3DA5C7AA269683E0UL)]
    [Arguments(9, 0x4B17A9BA9E215C09UL)]
    [Arguments(15, 0xAE2A37EB9357CAA7UL)]
    [Arguments(16, 0xA19AD429B02BC413UL)]
    [Arguments(31, 0x4A74F3A1A39AD4A1UL)]
    [Arguments(32, 0x8D57D6A4671CC43DUL)]
    [Arguments(33, 0x62C9FD21ED857664UL)]
    [Arguments(63, 0x5C320A0D2707057FUL)]
    [Arguments(64, 0x7BBABBC45729D17EUL)]
    [Arguments(100, 0xEFA0AD2D3E70C151UL)]
    [Arguments(1000, 0x99594F4828043D35UL)]
    public async Task PatternMatchesReference(int length, ulong expected)
    {
        var data = new byte[length];
        for (var i = 0; i < length; i++)
            data[i] = Convert.ToByte(((i * PatternMultiplier) + PatternOffset) & 0xFF);

        _ = await Assert.That(XxHash64.Hash(data)).IsEqualTo(expected);
    }
}
