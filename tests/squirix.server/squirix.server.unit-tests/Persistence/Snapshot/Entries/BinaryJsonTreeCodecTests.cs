using System.Text.Json;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot.Entries;

/// <summary>Unit tests for <see cref="BinaryJsonTreeCodec" />.</summary>
public sealed class BinaryJsonTreeCodecTests : UnitTestBase
{
    /// <summary>Nested object and array values round-trip through the binary tree codec.</summary>
    [Fact]
    public void RoundTripsNestedObjectAndArray()
    {
        using var document = JsonDocument.Parse("""{"name":"alice","scores":[1,2,3],"active":true}""");
        var element = document.RootElement;
        var buffer = new byte[BinaryJsonTreeCodec.ComputeEncodedLength(element)];
        var written = BinaryJsonTreeCodec.Write(element, buffer);

        Assert.Equal(buffer.Length, written);
        Assert.True(BinaryJsonTreeCodec.TryRead(buffer, out var roundTrip, out var bytesRead));
        Assert.Equal(written, bytesRead);
        Assert.Equal("alice", roundTrip.GetProperty("name").GetString());
        Assert.Equal(3, roundTrip.GetProperty("scores").GetArrayLength());
        Assert.True(roundTrip.GetProperty("active").GetBoolean());
    }
}
