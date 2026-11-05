using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Prepared local appends share owned payloads while external appends retain their buffer isolation.</summary>
[Immutable]
public sealed class ReplicaOwnedAppendTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

    /// <summary>A prepared batch retains the exact owned memories and produces the same file bytes as a copying append.</summary>
    /// <param name="notify">Whether scheduling starts early replication.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PreparedAppendSharesOwnedPayloads(bool notify, CancellationToken cancellationToken)
    {
        var ownedRoot = Path.Join(Dir, "owned");
        var externalRoot = Path.Join(Dir, "external");
        await using var owned = OpenLog(ownedRoot);
        await using var external = OpenLog(externalRoot);
        await owned.OpenSeededAsync(cancellationToken);
        await external.OpenSeededAsync(cancellationToken);
        var first = ReplicaPipelineTestKit.Mutation(1);
        var second = ReplicaPipelineTestKit.Mutation(2);
        var request = Prepare(first, second);
        var notifications = 0;
        Action? scheduled = notify ? () => { _ = Interlocked.Increment(ref notifications); } : null;

        _ = await Assert.That((await owned.AppendPreparedAsync(request, scheduled, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await external.AppendAsync(request.Request, cancellationToken)).Success).IsTrue();
        var ownedTail = await owned.GetUncommittedTailAsync(cancellationToken);
        var externalTail = await external.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(ownedTail.Count).IsEqualTo(2);
        _ = await Assert.That(externalTail.Count).IsEqualTo(2);
        _ = await Assert.That(ownedTail[0].Payload.Equals(first.CanonicalPayload)).IsTrue();
        _ = await Assert.That(ownedTail[1].Payload.Equals(second.CanonicalPayload)).IsTrue();
        _ = await Assert.That(externalTail[0].Payload.Equals(first.CanonicalPayload)).IsFalse();
        _ = await Assert.That(externalTail[1].Payload.Equals(second.CanonicalPayload)).IsFalse();
        await SequenceAssert.EqualMemoryAsync(first.CanonicalPayload, externalTail[0].Payload);
        await SequenceAssert.EqualMemoryAsync(second.CanonicalPayload, externalTail[1].Payload);
        _ = await Assert.That(notifications).IsEqualTo(notify ? 1 : 0);

        await owned.DisposeAsync();
        await external.DisposeAsync();
        var ownedBytes = await File.ReadAllBytesAsync(GroupLogFile.ActivePath(ownedRoot, GroupId), cancellationToken);
        var externalBytes = await File.ReadAllBytesAsync(GroupLogFile.ActivePath(externalRoot, GroupId), cancellationToken);
        await SequenceAssert.EqualMemoryAsync<byte>(externalBytes, ownedBytes);
    }

    /// <summary>Changing caller input and canceling after scheduling cannot change either append's durable bytes.</summary>
    /// <param name="prepared">Whether the request goes through a prepared mutation's copying constructor.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ScheduledAppendProtectsCallerBuffer(bool prepared, CancellationToken cancellationToken)
    {
        var original = ReplicaPipelineTestKit.Mutation(1);
        var caller = original.CanonicalPayload.ToArray();
        using var hooks = new StallableFollowerLogFaultHooks();
        await using (var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, hooks, TimeProvider.System))
        {
            await log.OpenSeededAsync(cancellationToken);
            hooks.StallNextFrameWrite();
            using var canceled = new CancellationTokenSource();
            var mutation = new PreparedReplicaMutation(
                new ReplicaOperationIdentity(GroupId, original.OperationScope, original.OperationId, original.OperationFingerprint),
                original.Term,
                original.LogIndex,
                new ReplicaMutationPayload(caller, original.OutcomePayload, original.PayloadChecksum));
            var request = Prepare(mutation);
            var append = prepared
                ? log.AppendPreparedAsync(request, null, canceled.Token)
                : log.AppendAsync(request.Request with { Entries = new FollowerLogEntry[] { new(1, 1, caller) } }, canceled.Token);
            try
            {
                await hooks.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);
                Array.Fill<byte>(caller, 255);
                await canceled.CancelAsync();
                _ = await Assert.That(append.IsCompleted).IsFalse();
            }
            finally
            {
                hooks.Release();
                Task completion = append;
                await completion.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }

            _ = await Assert.That((await append).Success).IsTrue();
            var tail = await log.GetUncommittedTailAsync(cancellationToken);
            await SequenceAssert.EqualMemoryAsync(original.CanonicalPayload, tail[0].Payload);
            _ = await Assert.That(tail[0].Payload.Equals(mutation.CanonicalPayload)).IsEqualTo(prepared);
        }

        await using var recovered = OpenLog(Dir);
        await recovered.OpenAsync(cancellationToken);
        var recoveredTail = await recovered.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(recoveredTail.Count).IsEqualTo(1);
        await SequenceAssert.EqualMemoryAsync(original.CanonicalPayload, recoveredTail[0].Payload);
    }

    /// <summary>A divergent uncommitted suffix is truncated before a prepared batch replaces it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreparedAppendReplacesDivergentTail(CancellationToken cancellationToken)
    {
        await using var log = OpenLog(Dir);
        await log.OpenSeededAsync(cancellationToken);
        var initial = Prepare(ReplicaPipelineTestKit.Mutation(1));
        _ = await Assert.That((await log.AppendAsync(initial.Request, cancellationToken)).Success).IsTrue();
        var replacement = ReplicaPipelineTestKit.Mutation(1, 2);
        var last = ReplicaPipelineTestKit.Mutation(2, 2);
        var request = Prepare(replacement, last);

        _ = await Assert.That((await log.AppendPreparedAsync(request, null, cancellationToken)).Success).IsTrue();
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(2);
        _ = await Assert.That((tail[0].LogIndex, tail[0].Term, tail[1].LogIndex, tail[1].Term)).IsEqualTo((1UL, 2UL, 2UL, 2UL));
        _ = await Assert.That(tail[0].Payload.Equals(replacement.CanonicalPayload)).IsTrue();
        _ = await Assert.That(tail[1].Payload.Equals(last.CanonicalPayload)).IsTrue();
    }

    /// <summary>The log retains payload memory after the prepared mutation and request are no longer reachable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [SuppressMessage("Major Code Smell", "S1215", Justification = "Collection verifies the distinct lifetimes of a prepared mutation and its retained payload.")]
    public async Task LogKeepsPayloadAfterMutationCollection(CancellationToken cancellationToken)
    {
        await using var log = OpenLog(Dir);
        await log.OpenSeededAsync(cancellationToken);
        var request = CreateRequest(out var mutation, out var payload);
        _ = await Assert.That((await log.AppendPreparedAsync(request, null, cancellationToken)).Success).IsTrue();
        request = default;

        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);

        _ = await Assert.That(mutation.IsAlive).IsFalse();
        _ = await Assert.That(payload.IsAlive).IsTrue();
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(1);
        await SequenceAssert.EqualMemoryAsync(ReplicaPipelineTestKit.Mutation(1).CanonicalPayload, tail[0].Payload);
        GC.KeepAlive(log);
    }

    private static FollowerLog OpenLog(string root) => new(root, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static PreparedLogAppendRequest CreateRequest(out WeakReference mutation, out WeakReference payload)
    {
        var prepared = ReplicaPipelineTestKit.Mutation(1);
        _ = System.Runtime.InteropServices.MemoryMarshal.TryGetArray(prepared.CanonicalPayload, out var buffer);
        mutation = new WeakReference(prepared);
        payload = new WeakReference(buffer.Array);
        return Prepare(prepared);
    }

    private static PreparedLogAppendRequest Prepare(params PreparedReplicaMutation[] mutations)
    {
        var entries = new FollowerLogEntry[mutations.Length];
        for (var i = 0; i < entries.Length; i++)
            entries[i] = new FollowerLogEntry(mutations[i].LogIndex, mutations[i].Term, mutations[i].CanonicalPayload);

        var request = new FollowerLogAppendRequest("leader", mutations[0].Term, 0, 0, 0, entries);
        return PreparedLogAppendRequest.TakeOwnership(in request);
    }
}
