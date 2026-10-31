using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Replay payload serialization shares empty storage and preserves nonempty protobuf data.</summary>
[Immutable]
public sealed class IdempotencyResponseBufferTests : ServerUnitTestBase
{
    /// <summary>Empty Set outcomes use shared storage and replay as a successful empty response.</summary>
    [Test]
    public async Task EmptySetSharesBufferAndReplays()
    {
        var first = IdempotencyResponseCodec.SerializeResponseBytes(new SetAsyncResponse());
        var second = IdempotencyResponseCodec.SerializeResponseBytes(new SetAsyncResponse());

        _ = await Assert.That(first).IsSameReferenceAs(Array.Empty<byte>());
        _ = await Assert.That(second).IsSameReferenceAs(first);
        _ = await Assert.That(first.Length).IsEqualTo(0);
        _ = await Assert.That(SetAsyncResponse.Parser.ParseFrom(first)).IsEqualTo(new SetAsyncResponse());
    }

    /// <summary>Nonempty outcome bytes remain owned independently and retain their wire fields.</summary>
    [Test]
    public async Task NonemptyResponseOwnsBytes()
    {
        var response = new TouchAsyncResponse { Found = true };
        var first = IdempotencyResponseCodec.SerializeResponseBytes(response);
        response.Found = false;
        var second = IdempotencyResponseCodec.SerializeResponseBytes(new TouchAsyncResponse { Found = true });

        _ = await Assert.That(first.Length).IsEqualTo(2);
        _ = await Assert.That(first[0]).IsEqualTo(byte.CreateChecked(8));
        _ = await Assert.That(first[1]).IsEqualTo(byte.CreateChecked(1));
        _ = await Assert.That(first).IsNotSameReferenceAs(second);
        _ = await Assert.That(TouchAsyncResponse.Parser.ParseFrom(first).Found).IsTrue();
        first[1] = 0;
        _ = await Assert.That(TouchAsyncResponse.Parser.ParseFrom(second).Found).IsTrue();
    }
}
