using System;
using System.IO;
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

/// <summary>Replica log appends retain immutable payloads supplied by prepared mutations.</summary>
[Immutable]
public sealed class ReplicaLogPayloadTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

    /// <summary>Both append modes retain the exact payload memories and produce identical log files.</summary>
    /// <param name="notify">Whether scheduling starts early replication.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AppendRetainsPayloadWithoutCopy(bool notify, CancellationToken cancellationToken)
    {
        var firstRoot = Path.Join(Dir, "first");
        var secondRoot = Path.Join(Dir, "second");
        await using var firstLog = OpenLog(firstRoot);
        await using var secondLog = OpenLog(secondRoot);
        await firstLog.OpenSeededAsync(cancellationToken);
        await secondLog.OpenSeededAsync(cancellationToken);
        var first = ReplicaPipelineTestKit.Mutation(1);
        var second = ReplicaPipelineTestKit.Mutation(2);
        var request = Request(first, second);
        var notifications = 0;
        var append = notify
            ? firstLog.AppendLeaderAsync(request, () => { _ = Interlocked.Increment(ref notifications); }, cancellationToken)
            : firstLog.AppendAsync(request, cancellationToken);
        _ = await Assert.That((await append).Success).IsTrue();
        _ = await Assert.That((await secondLog.AppendAsync(request, cancellationToken)).Success).IsTrue();

        var firstTail = await firstLog.GetUncommittedTailAsync(cancellationToken);
        var secondTail = await secondLog.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(firstTail.Count).IsEqualTo(2);
        _ = await Assert.That(secondTail.Count).IsEqualTo(2);
        _ = await Assert.That(firstTail[0].Payload.Equals(first.CanonicalPayload)).IsTrue();
        _ = await Assert.That(firstTail[1].Payload.Equals(second.CanonicalPayload)).IsTrue();
        _ = await Assert.That(secondTail[0].Payload.Equals(first.CanonicalPayload)).IsTrue();
        _ = await Assert.That(secondTail[1].Payload.Equals(second.CanonicalPayload)).IsTrue();
        _ = await Assert.That(notifications).IsEqualTo(notify ? 1 : 0);

        await firstLog.DisposeAsync();
        await secondLog.DisposeAsync();
        var firstBytes = await File.ReadAllBytesAsync(GroupLogFile.ActivePath(firstRoot, GroupId), cancellationToken);
        var secondBytes = await File.ReadAllBytesAsync(GroupLogFile.ActivePath(secondRoot, GroupId), cancellationToken);
        await SequenceAssert.EqualMemoryAsync<byte>(firstBytes, secondBytes);
    }

    /// <summary>The mutation constructor isolates external input, including during cancellation after storage scheduling.</summary>
    /// <param name="notify">Whether the append sends a scheduling notification.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MutationCopySurvivesCallerChanges(bool notify, CancellationToken cancellationToken)
    {
        var original = ReplicaPipelineTestKit.Mutation(1);
        var caller = original.CanonicalPayload.ToArray();
        var mutation = new PreparedReplicaMutation(
            new ReplicaOperationIdentity(GroupId, original.OperationScope, original.OperationId, original.OperationFingerprint),
            original.Term,
            original.LogIndex,
            new ReplicaMutationPayload(caller, original.OutcomePayload, original.PayloadChecksum));
        Array.Fill<byte>(caller, 255);
        using var hooks = new StallableFollowerLogFaultHooks();
        await using (var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, hooks, TimeProvider.System))
        {
            await log.OpenSeededAsync(cancellationToken);
            hooks.StallNextFrameWrite();
            using var canceled = new CancellationTokenSource();
            var request = Request(mutation);
            var notifications = 0;
            var append = notify
                ? log.AppendLeaderAsync(request, () => { _ = Interlocked.Increment(ref notifications); }, canceled.Token)
                : log.AppendAsync(request, canceled.Token);
            try
            {
                await hooks.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);
                Array.Fill<byte>(caller, 0);
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
            _ = await Assert.That(notifications).IsEqualTo(notify ? 1 : 0);
            var tail = await log.GetUncommittedTailAsync(cancellationToken);
            _ = await Assert.That(tail.Count).IsEqualTo(1);
            await SequenceAssert.EqualMemoryAsync(original.CanonicalPayload, tail[0].Payload);
            _ = await Assert.That(tail[0].Payload.Equals(mutation.CanonicalPayload)).IsTrue();
        }

        await using var recovered = OpenLog(Dir);
        await recovered.OpenAsync(cancellationToken);
        var recoveredTail = await recovered.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(recoveredTail.Count).IsEqualTo(1);
        await SequenceAssert.EqualMemoryAsync(original.CanonicalPayload, recoveredTail[0].Payload);
    }

    /// <summary>A divergent uncommitted suffix is replaced without copying the new immutable payloads.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendReplacesDivergentTail(CancellationToken cancellationToken)
    {
        await using var log = OpenLog(Dir);
        await log.OpenSeededAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Request(ReplicaPipelineTestKit.Mutation(1)), cancellationToken)).Success).IsTrue();
        var replacement = ReplicaPipelineTestKit.Mutation(1, 2);
        var last = ReplicaPipelineTestKit.Mutation(2, 2);

        _ = await Assert.That((await log.AppendAsync(Request(replacement, last), cancellationToken)).Success).IsTrue();
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(2);
        _ = await Assert.That((tail[0].LogIndex, tail[0].Term, tail[1].LogIndex, tail[1].Term)).IsEqualTo((1UL, 2UL, 2UL, 2UL));
        _ = await Assert.That(tail[0].Payload.Equals(replacement.CanonicalPayload)).IsTrue();
        _ = await Assert.That(tail[1].Payload.Equals(last.CanonicalPayload)).IsTrue();
    }

    private static FollowerLog OpenLog(string root) => new(root, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);

    private static FollowerLogAppendRequest Request(params PreparedReplicaMutation[] mutations)
    {
        var entries = new FollowerLogEntry[mutations.Length];
        for (var i = 0; i < entries.Length; i++)
            entries[i] = new FollowerLogEntry(mutations[i].LogIndex, mutations[i].Term, mutations[i].CanonicalPayload);

        return new FollowerLogAppendRequest("leader", mutations[0].Term, 0, 0, 0, entries);
    }
}
