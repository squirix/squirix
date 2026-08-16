using System.Text;
using Xunit;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Covers protocol-model JSON string escaping helpers.</summary>
public static class JsonTextTests
{
    /// <summary>Control characters below space are escaped as \\uXXXX.</summary>
    [Fact]
    public static void AppendStringEscapesControlCharacters()
    {
        var sb = new StringBuilder();
        JsonText.AppendString(sb, "a\u0001b\"c\\d");
        Assert.Equal("\"a\\u0001b\\\"c\\\\d\"", sb.ToString());
    }
}
