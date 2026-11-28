using System;
using System.IO;
using System.Security.Claims;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Endpoint.Resp;
using Squirix.Server.Composition;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies ordered batching and flush boundaries of RESP replies.</summary>
public sealed class RespBatchTests
{
    /// <summary>Complete buffered commands share one write, including request-backed binary arguments.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task BatchesBufferedRepliesInOrder(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        var services = host.Server.Services;
        var cache = new CacheCommands(services.Require<BinaryDispatch>(), null, TimeProvider.System, false);
        var session = new Session(null, cache, 1, TimeProvider.System);
        byte[] requests = [.. "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$3\r\n"u8, 0, 255, 13, .. "\r\n*1\r\n$4\r\nPING\r\n"u8];
        await using var stream = new RecordingReplyStream(requests, int.MaxValue);

        await session.RunAsync(stream, cancellationToken, cancellationToken);

        byte[] expected = [.. "+PONG\r\n$3\r\n"u8, 0, 255, 13, .. "\r\n+PONG\r\n"u8];
        _ = await Assert.That(stream.Writes).IsEqualTo(1);
        _ = await Assert.That(stream.Output.AsSpan().SequenceEqual(expected)).IsTrue();
    }

    /// <summary>An incomplete next request does not delay a ready response.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task FlushesBeforeReadingFragmentedRequest(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        var services = host.Server.Services;
        var cache = new CacheCommands(services.Require<BinaryDispatch>(), null, TimeProvider.System, false);
        var session = new Session(null, cache, 1, TimeProvider.System);
        var requests = Encoding.ASCII.GetBytes(RespTestHost.Command("PING") + RespTestHost.Command("PING"));
        await using var stream = new RecordingReplyStream(requests, RespTestHost.Command("PING").Length + 2);

        await session.RunAsync(stream, cancellationToken, cancellationToken);

        _ = await Assert.That(stream.Writes).IsEqualTo(2);
        _ = await Assert.That(Encoding.ASCII.GetString(stream.Output)).IsEqualTo("+PONG\r\n+PONG\r\n");
        _ = await Assert.That(stream.WritesAtSecondRead).IsEqualTo(1);
    }

    /// <summary>A framing error follows all ready responses and closes the connection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task RepliesPrecedeProtocolError(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        var services = host.Server.Services;
        var cache = new CacheCommands(services.Require<BinaryDispatch>(), null, TimeProvider.System, false);
        var session = new Session(null, cache, 1, TimeProvider.System);
        byte[] requests = [.. "*1\r\n$4\r\nPING\r\n*0\r\n*1\r\n$4\r\nPING\r\n"u8];
        await using var stream = new RecordingReplyStream(requests, int.MaxValue);

        await session.RunAsync(stream, cancellationToken, cancellationToken);

        _ = await Assert.That(stream.Writes).IsEqualTo(1);
        _ = await Assert.That(Encoding.ASCII.GetString(stream.Output)).IsEqualTo("+PONG\r\n-ERR invalid RESP request framing\r\n");
    }

    /// <summary>A pending authentication operation cannot hold an earlier ready response.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task FlushesBeforePendingCommandCompletes(CancellationToken cancellationToken)
    {
        var observer = new BlockRenewal();
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var token = TestJwtHelper.CreateBearerToken(credentials);
        await using var host = await RespTestHost.StartAsync(cancellationToken, jwt: credentials, register: services =>
            _ = services.AddSingleton<IConnectionTokenObserver>(observer));
        var services = host.Server.Services;
        var validator = services.Require<NativeConnectionAuthenticationValidator>();
        var cache = new CacheCommands(services.Require<BinaryDispatch>(), validator, TimeProvider.System, false);
        var session = new Session(validator, cache, 1, TimeProvider.System);
        var requests = RespTestHost.Command("AUTH", token) + RespTestHost.Command("PING") + RespTestHost.Command("AUTH", token);
        await using var stream = new RecordingReplyStream(Encoding.ASCII.GetBytes(requests), int.MaxValue);
#pragma warning disable CA2025 // The session is awaited in the finally block before stream disposal.
        var running = session.RunAsync(stream, cancellationToken, cancellationToken);
#pragma warning restore CA2025
        try
        {
            await observer.Entered.Task.WaitAsync(cancellationToken);
            await stream.SecondWrite.Task.WaitAsync(cancellationToken);
            _ = await Assert.That(Encoding.ASCII.GetString(stream.Output)).IsEqualTo("+OK\r\n+PONG\r\n");
            _ = await Assert.That(running.IsCompleted).IsFalse();
        }
        finally
        {
            _ = observer.Release.TrySetResult();
            await running;
        }

        _ = await Assert.That(Encoding.ASCII.GetString(stream.Output)).IsEqualTo("+OK\r\n+PONG\r\n+OK\r\n");
    }

    /// <summary>A failed batch write cancels and observes the command already using the request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task WriteFailureCancelsPendingCommand(CancellationToken cancellationToken)
    {
        var observer = new BlockRenewal();
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var token = TestJwtHelper.CreateBearerToken(credentials);
        await using var host = await RespTestHost.StartAsync(cancellationToken, jwt: credentials, register: services =>
            _ = services.AddSingleton<IConnectionTokenObserver>(observer));
        var services = host.Server.Services;
        var validator = services.Require<NativeConnectionAuthenticationValidator>();
        var cache = new CacheCommands(services.Require<BinaryDispatch>(), validator, TimeProvider.System, false);
        var session = new Session(validator, cache, 1, TimeProvider.System);
        var requests = RespTestHost.Command("AUTH", token) + RespTestHost.Command("PING") + RespTestHost.Command("AUTH", token);
        await using var stream = new RecordingReplyStream(Encoding.ASCII.GetBytes(requests), int.MaxValue, true);

        var error = await NodeAsyncAssert.ThrowsAsync<IOException>(session.RunAsync(stream, cancellationToken, cancellationToken), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo("Write failed.");
        _ = await Assert.That(observer.Cancelled.Task.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(Encoding.ASCII.GetString(stream.Output)).IsEqualTo("+OK\r\n");
    }

    private sealed class BlockRenewal : IConnectionTokenObserver
    {
        private int _calls;

        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask<ClaimsPrincipal?> OnValidatedAsync(ClaimsPrincipal principal, IServiceProvider services, BinaryConnection? connection, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _calls) == 2)
            {
                _ = Entered.TrySetResult();
                try
                {
                    await Release.Task.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _ = Cancelled.TrySetResult();
                    throw;
                }
            }

            return principal;
        }
    }

    private sealed class RecordingReplyStream : MemoryStream
    {
        private readonly MemoryStream _output = new();
        private readonly int _readLimit;
        private readonly bool _failSecondWrite;
        private int _reads;

        internal RecordingReplyStream(byte[] requests, int readLimit, bool failSecondWrite = false)
            : base(requests, false)
        {
            _readLimit = readLimit;
            _failSecondWrite = failSecondWrite;
        }

        internal int Writes { get; private set; }

        internal TaskCompletionSource SecondWrite { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int WritesAtSecondRead { get; private set; }

        internal byte[] Output => _output.ToArray();

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (++_reads == 2)
                WritesAtSecondRead = Writes;
            return base.ReadAsync(buffer[..Math.Min(buffer.Length, _readLimit)], cancellationToken);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes++;
            if (Writes == 2 && _failSecondWrite)
                return ValueTask.FromException(new IOException("Write failed."));
            var write = _output.WriteAsync(buffer, cancellationToken);
            if (Writes == 2)
                _ = SecondWrite.TrySetResult();
            return write;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _output.Dispose();
            base.Dispose(disposing);
        }
    }
}
