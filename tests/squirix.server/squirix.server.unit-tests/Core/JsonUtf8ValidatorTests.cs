using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Tests for <see cref="JsonUtf8Validator" />.</summary>
[Immutable]
public sealed class JsonUtf8ValidatorTests : ServerUnitTestBase
{
    /// <summary>One well-formed JSON value of any kind, with or without surrounding white space, is valid.</summary>
    /// <param name="json">The JSON text.</param>
    [Test]
    [Arguments("{}")]
    [Arguments("[]")]
    [Arguments("0")]
    [Arguments("-1.5e+10")]
    [Arguments("12345678901234567890")]
    [Arguments("\"text\"")]
    [Arguments("\"\\u00e9 \\n é 😀\"")]
    [Arguments("true")]
    [Arguments("false")]
    [Arguments("null")]
    [Arguments(" \t\r\n{\"a\":[1,{\"b\":null}]}\n ")]
    [Arguments("{\"a\":1,\"a\":2}")]
    public async Task ValidValueIsAccepted(string json)
    {
        var bytes = Encoding.UTF8.GetBytes(json);

        _ = await Assert.That(JsonUtf8Validator.IsValid(bytes)).IsTrue();
        _ = await Assert.That(JsonUtf8Value.TryCreate(bytes, out _)).IsTrue();
    }

    /// <summary>Anything that is not exactly one well-formed JSON value is invalid.</summary>
    /// <param name="json">The text, encoded as UTF-8.</param>
    [Test]
    [Arguments("")]
    [Arguments("   ")]
    [Arguments("{} {}")]
    [Arguments("1 2")]
    [Arguments("{}x")]
    [Arguments("garbage")]
    [Arguments("{\"a\":1} // comment")]
    [Arguments("/* c */ {}")]
    [Arguments("{\"a\":1,}")]
    [Arguments("[1,]")]
    [Arguments("NaN")]
    [Arguments("Infinity")]
    [Arguments("{\"a\":")]
    [Arguments("[1")]
    [Arguments("\"unterminated")]
    [Arguments("\"raw\ttab\"")]
    [Arguments("'single'")]
    [Arguments("{a:1}")]
    [Arguments("01")]
    [Arguments("1.")]
    [Arguments("+1")]
    public async Task MalformedValueIsRejected(string json)
    {
        var bytes = Encoding.UTF8.GetBytes(json);

        _ = await Assert.That(JsonUtf8Validator.IsValid(bytes)).IsFalse();
        _ = await Assert.That(JsonUtf8Value.TryCreate(bytes, out _)).IsFalse();
    }

    /// <summary>A byte order mark in front of the value is rejected.</summary>
    [Test]
    public async Task ByteOrderMarkIsRejected()
    {
        byte[] bytes = [0xEF, 0xBB, 0xBF, 0x7B, 0x7D];

        _ = await Assert.That(JsonUtf8Validator.IsValid(bytes)).IsFalse();
    }

    /// <summary>Bytes that are not valid UTF-8 are rejected, inside a string and outside it.</summary>
    [Test]
    public async Task InvalidUtf8IsRejected()
    {
        byte[] insideString = [0x22, 0xC3, 0x28, 0x22];
        byte[] truncatedSequence = [0x22, 0xE2, 0x82, 0x22];
        byte[] overlong = [0x22, 0xC0, 0xAF, 0x22];

        _ = await Assert.That(JsonUtf8Validator.IsValid(insideString)).IsFalse();
        _ = await Assert.That(JsonUtf8Validator.IsValid(truncatedSequence)).IsFalse();
        _ = await Assert.That(JsonUtf8Validator.IsValid(overlong)).IsFalse();
    }

    /// <summary>A value nested 64 deep is valid and one nested 65 deep is not.</summary>
    [Test]
    public async Task DepthLimitIsSixtyFour()
    {
        var atLimit = new string('[', JsonUtf8Validator.MaxDepth) + new string(']', JsonUtf8Validator.MaxDepth);
        var beyond = new string('[', JsonUtf8Validator.MaxDepth + 1) + new string(']', JsonUtf8Validator.MaxDepth + 1);

        _ = await Assert.That(JsonUtf8Validator.IsValid(Encoding.UTF8.GetBytes(atLimit))).IsTrue();
        _ = await Assert.That(JsonUtf8Validator.IsValid(Encoding.UTF8.GetBytes(beyond))).IsFalse();
    }
}
