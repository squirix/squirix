using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A prepared mutation copies the buffers it is given, unless its builder hands over buffers nobody else holds.</summary>
public sealed class PreparedReplicaMutationTests : ServerUnitTestBase
{
    private const uint Checksum = 7;
    private const ulong LogIndex = 3;
    private const ulong Term = 2;
    private static readonly byte[] Canonical = [1, 2, 3];
    private static readonly byte[] Fingerprint = [4, 5];
    private static readonly byte[] Outcome = [6];

    /// <summary>The public constructor stores private copies of all three buffers.</summary>
    [Test]
    public async Task ConstructorCopiesBuffers()
    {
        var mutation = new PreparedReplicaMutation(NewIdentity(), Term, LogIndex, NewPayload());

        _ = await Assert.That(SharesStorage(mutation.CanonicalPayload, Canonical)).IsFalse();
        _ = await Assert.That(SharesStorage(mutation.OperationFingerprint, Fingerprint)).IsFalse();
        _ = await Assert.That(SharesStorage(mutation.OutcomePayload, Outcome)).IsFalse();
        _ = await Assert.That(mutation.CanonicalPayload.Span.SequenceEqual(Canonical)).IsTrue();
        _ = await Assert.That(mutation.OperationFingerprint.Span.SequenceEqual(Fingerprint)).IsTrue();
        _ = await Assert.That(mutation.OutcomePayload.Span.SequenceEqual(Outcome)).IsTrue();
    }

    /// <summary>Taking ownership stores the very buffers it is given and validates the identity like the constructor.</summary>
    [Test]
    public async Task TakeOwnershipStoresGivenBuffers()
    {
        var mutation = PreparedReplicaMutation.TakeOwnership(NewIdentity(), Term, LogIndex, NewPayload());

        _ = await Assert.That(SharesStorage(mutation.CanonicalPayload, Canonical)).IsTrue();
        _ = await Assert.That(SharesStorage(mutation.OperationFingerprint, Fingerprint)).IsTrue();
        _ = await Assert.That(SharesStorage(mutation.OutcomePayload, Outcome)).IsTrue();
        _ = await Assert.That(mutation.PayloadChecksum).IsEqualTo(Checksum);
        _ = await Assert.That(mutation.Term).IsEqualTo(Term);
        _ = await Assert.That(mutation.LogIndex).IsEqualTo(LogIndex);
    }

    /// <summary>A mutation built by the leader factory holds the exact-size array the codec produced, not a copy of it.</summary>
    [Test]
    public async Task FactoryBuildKeepsEncodedArray()
    {
        var factory = new ReplicaMutationFactory(new StubCache(), "g1", 1UL, new FakeTimeProvider(), NullLogger.Instance);

        var mutation = factory.PrepareLeaderTerm(1UL);

        _ = await Assert.That(MemoryMarshal.TryGetArray(mutation.CanonicalPayload, out var canonical)).IsTrue();
        _ = await Assert.That(canonical.Offset).IsEqualTo(0);
        _ = await Assert.That(canonical.Count).IsEqualTo(canonical.Array!.Length);
        _ = await Assert.That(ReplicaLogCodec.Decode(canonical.Array)).IsNotNull();
        _ = await Assert.That(mutation.PayloadChecksum).IsEqualTo(Crc32C.Compute(canonical.Array));
    }

    private static ReplicaOperationIdentity NewIdentity() => new("g1", "scope", "op", Fingerprint);

    private static ReplicaMutationPayload NewPayload() => new(Canonical, Outcome, Checksum);

    private static bool SharesStorage(ReadOnlyMemory<byte> memory, byte[] array) =>
        MemoryMarshal.TryGetArray(memory, out var segment) && ReferenceEquals(segment.Array, array);
}
