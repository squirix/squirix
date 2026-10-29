using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Pooled transfer parsing preserves protobuf fields while borrowing one owned message buffer.</summary>
public sealed class ReplicaTransferParsingTests
{
    /// <summary>Entry bytes share the rented message buffer and are returned exactly once after processing.</summary>
    [Test]
    public async Task EntryBytesBorrowMessageBuffer()
    {
        var request = new TransferReplicaStateRequest
        {
            Header = new ReplicationEnvelopeHeader { GroupId = "group", LeaderNodeId = "leader", Term = 2UL },
            TransferId = "transfer",
            Sequence = 3U,
            Entries = new ReplicaTransferEntries(),
        };
        request.Entries.Entries.Add(new ReplicaTransferEntry { CacheName = "cache", Key = ByteString.CopyFromUtf8("key"), Payload = ByteString.CopyFromUtf8("payload"), ExpiresUtcTicks = 42L });
        var pool = new TrackingPool();
        var lease = ReplicaTransferWire.Parse(new ReadOnlySequence<byte>(request.ToByteArray()), pool);
        var equal = lease.Request.Equals(request);
        var payload = lease.Request.Entries.Entries[0].Payload.Memory;
        var borrowed = System.Runtime.InteropServices.MemoryMarshal.TryGetArray(payload, out var segment) && ReferenceEquals(segment.Array, pool.Buffer);
        var returnsWhileOwned = pool.Returns;

        lease.Dispose();
        lease.Dispose();

        _ = await Assert.That(equal && borrowed).IsTrue();
        _ = await Assert.That((pool.Rents, returnsWhileOwned, pool.Returns)).IsEqualTo((1, 0, 1));
    }

    /// <summary>A parse failure returns its rented buffer without waiting for the handler.</summary>
    [Test]
    public async Task MalformedMessageReturnsBuffer()
    {
        byte[] bytes = [0x3A, 0x05, 0x0A];
        var pool = new TrackingPool();

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws((Bytes: bytes, Pool: pool), static s => _ = ReplicaTransferWire.Parse(new ReadOnlySequence<byte>(s.Bytes), s.Pool));

        _ = await Assert.That((pool.Rents, pool.Returns)).IsEqualTo((1, 1));
    }

    /// <summary>Cancellation does not return a buffer while a handler may still read it; HTTP completion does even without dispatch.</summary>
    [Test]
    public async Task CompletionOwnsBufferAfterCancellation()
    {
        var response = new CompletionFeature();
        var http = new DefaultHttpContext();
        http.Features.Set<IHttpResponseFeature>(response);
        using var cancellation = new CancellationTokenSource();
        http.RequestAborted = cancellation.Token;
        var pool = new TrackingPool();
        var marshaller = new ReplicaTransferMarshaller(new HttpContextAccessor { HttpContext = http }, pool);
        var original = new TransferReplicaStateRequest { Snapshot = new ReplicaTransferSnapshot { Chunk = ByteString.CopyFromUtf8("snapshot") } };
        var request = marshaller.Deserialize(new PayloadContext(original.ToByteArray()));

        await cancellation.CancelAsync();
        _ = await Assert.That(pool.Returns).IsEqualTo(0);
        _ = await Assert.That(request.Equals(original)).IsTrue();
        await response.CompleteAsync();

        _ = await Assert.That(pool.Returns).IsEqualTo(1);
    }

    /// <summary>The parser accepts segmented transport input and no longer borrows that input when it returns.</summary>
    [Test]
    public async Task SegmentedInputIsCopiedIntoOwnedBuffer()
    {
        var original = new TransferReplicaStateRequest { Snapshot = new ReplicaTransferSnapshot { Chunk = ByteString.CopyFromUtf8("snapshot") } };
        var bytes = original.ToByteArray();
        var first = new PayloadSegment(bytes.AsMemory(0, 3));
        var last = first.Append(bytes.AsMemory(3));
        var sequence = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        using var parsed = ReplicaTransferWire.Parse(in sequence, ArrayPool<byte>.Shared);
        bytes.AsSpan().Fill(0xCC);

        _ = await Assert.That(parsed.Request.Equals(original)).IsTrue();
    }

    private sealed class CompletionFeature : HttpResponseFeature
    {
        private Func<object, Task>? _callback;
        private object? _state;

        public override void OnCompleted(Func<object, Task> callback, object state)
        {
            _callback = callback;
            _state = state;
        }

        internal Task CompleteAsync() => _callback!(_state!);
    }

    private sealed class PayloadContext : DeserializationContext
    {
        private readonly byte[] _bytes;

        internal PayloadContext(byte[] bytes)
        {
            _bytes = bytes;
        }

        public override int PayloadLength => _bytes.Length;

        public override byte[] PayloadAsNewBuffer() => throw new NotSupportedException();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => new(_bytes);
    }

    private sealed class PayloadSegment : ReadOnlySequenceSegment<byte>
    {
        internal PayloadSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        internal PayloadSegment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new PayloadSegment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }

    private sealed class TrackingPool : ArrayPool<byte>
    {
        internal byte[]? Buffer { get; private set; }

        internal int Rents { get; private set; }

        internal int Returns { get; private set; }

        public override byte[] Rent(int minimumLength)
        {
            Rents++;
            Buffer = new byte[minimumLength];
            return Buffer;
        }

        public override void Return(byte[] array, bool clearArray = false)
        {
            Returns++;
            array.AsSpan().Fill(0xCC);
        }
    }
}
