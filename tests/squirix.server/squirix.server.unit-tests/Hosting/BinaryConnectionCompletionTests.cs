using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies stream-backed output completion on the public transport.</summary>
public sealed class BinaryConnectionCompletionTests
{
    /// <summary>Graceful completion flushes pending output, while a connection abort discards it.</summary>
    /// <param name="abort">Whether the session ended with a forced abort.</param>
    /// <param name="expectedBytes">The number of bytes delivered during completion.</param>
    [Test]
    [Arguments(false, 1L)]
    [Arguments(true, 0L)]
    public async Task CompletionHonorsSessionOutcome(bool abort, long expectedBytes)
    {
        await using var destination = new MemoryStream();
        var pipe = new NativeBinaryEndpoint.StreamPipe(destination);
        using var connection = new BinaryConnection(pipe, null, null, CancellationToken.None, CancellationToken.None);
        pipe.Output.GetSpan(1)[0] = 42;
        pipe.Output.Advance(1);
        if (abort)
            connection.Abort();
        else
            connection.RequestClose();

        await connection.CompleteTransportAsync();
        _ = await Assert.That(destination.Length).IsEqualTo(expectedBytes);
    }
}
