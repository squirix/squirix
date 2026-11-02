using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Rocks;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Lease ownership while cancellation is still completing an underlying response read.</summary>
public sealed class ReplicaAppendEpochTests : ServerUnitTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>Response headers do not release the channel lease while its response read still runs.</summary>
    /// <param name="active">Whether an exchange must also end before the lease can drain.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CloseRetainsLeaseUntilReadEnds(bool active, CancellationToken cancellationToken)
    {
        await using var stream = new HeldResponseStream();
        using var handler = new ResponseHandler(stream);
        using var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions { HttpHandler = handler });
        var calls = new QuiescenceGate();
        calls.Enter();
        var pool = new IServerClientPoolCreateExpectations();
        _ = pool.Setups.LeaseChannel("peer", Arg.Any<CancellationToken>()).ReturnValue(new ServerChannelLease(channel, null, calls, CancellationToken.None));
        var epoch = new ReplicaAppendEpoch(pool.Instance(), "peer");
        if (active)
            _ = await Assert.That(epoch.Begin()).IsTrue();

        var reading = epoch.ReadAsync();
        try
        {
            await stream.Entered.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
            _ = await epoch.Call.ResponseHeadersAsync.WaitAsync(Bound, TimeProvider.System, cancellationToken);
            _ = await Assert.That(reading.IsCompleted).IsFalse();

            epoch.Close();
            if (active)
            {
                _ = await Assert.That(calls.Pending).IsEqualTo(1);
                epoch.End();
            }

            _ = await Assert.That(reading.IsCompleted).IsFalse();
            _ = await Assert.That(calls.Pending).IsEqualTo(1);
            _ = stream.Release.TrySetResult();
            _ = await reading.WaitAsync(Bound, TimeProvider.System, cancellationToken);
            await calls.WaitAsync(cancellationToken).AsTask().WaitAsync(Bound, TimeProvider.System, cancellationToken);
            epoch.Close();
            _ = await Assert.That(calls.Pending).IsEqualTo(0);
            pool.Verify();
        }
        finally
        {
            _ = stream.Release.TrySetResult();
            epoch.Close();
            if (active)
                epoch.End();
            _ = await reading.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        }
    }

    private sealed class ResponseHandler : HttpMessageHandler
    {
        private readonly HeldResponseStream _stream;

        internal ResponseHandler(HeldResponseStream stream)
        {
            _stream = stream;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK) { Version = HttpVersion.Version20, Content = new StreamContent(_stream) };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
            return Task.FromResult(response);
        }
    }

    /// <summary>Cancellation requests may precede completion of the asynchronous transport read.</summary>
    private sealed class HeldResponseStream : Stream
    {
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override void Flush() => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _ = Entered.TrySetResult();
            await Release.Task.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return 0;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
