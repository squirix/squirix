using System.Text;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Covers protocol-model JSON string escaping helpers.</summary>
public sealed class JsonTextTests
{
    /// <summary>Control characters below space are escaped as \\uXXXX.</summary>
    [Test]
    public async Task AppendStringEscapesControlCharacters()
    {
        var sb = new StringBuilder();
        JsonText.AppendString(sb, "a\u0001b\"c\\d");
        _ = await Assert.That(sb.ToString()).IsEqualTo("\"a\\u0001b\\\"c\\\\d\"");
    }
}
