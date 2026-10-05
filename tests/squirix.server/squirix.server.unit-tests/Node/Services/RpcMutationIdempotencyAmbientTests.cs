using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Runtime;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for the idempotency execution ambient stack.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyAmbientTests
{
    /// <summary>Deactivating a foreign scope leaves the active scope intact.</summary>
    [Test]
    public async Task DeactivateMismatchKeepsScope()
    {
        var active = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(active, "op-1", "fp-1");
        try
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(new object());
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsNotNull();
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsEqualTo("op-1");
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(active);
        }

        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsNull();
    }

    /// <summary>Notifying an applied mutation under a suspension that has no scope of its own does not mark any scope.</summary>
    [Test]
    public async Task ScopelessSuspensionKeepsScopesClean()
    {
        var finished = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(finished, "op-finished", "fp-finished");
        RpcMutationIdempotencyExecutionAmbient.Deactivate(finished);
        bool takenEffectInside;
        using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
        {
            RpcMutationIdempotencyExecutionAmbient.NotifyMutationApplied();
            takenEffectInside = RpcMutationIdempotencyExecutionAmbient.HasTakenEffect(finished);
        }

        _ = await Assert.That(takenEffectInside).IsFalse();
        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasTakenEffect(finished)).IsFalse();
        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended).IsFalse();
    }

    /// <summary>Suspending without a scope reports a replicated apply with no operation id, and disposing restores the empty state.</summary>
    [Test]
    public async Task SuspendWithoutScopeMarksReplicatedApply()
    {
        bool suspended;
        string? operationId;
        using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
        {
            suspended = RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended;
            operationId = RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue;
        }

        _ = await Assert.That(suspended).IsTrue();
        _ = await Assert.That(operationId).IsNull();
        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended).IsFalse();
        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsNull();
    }

    /// <summary>Suspending inside a scope and disposing restores the scope frame, which stamps again.</summary>
    [Test]
    public async Task SuspendInsideScopeRestoresScopeFrame()
    {
        var scope = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(scope, "op-1", "fp-1");
        try
        {
            using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
            {
                _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended).IsTrue();
                _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsNull();
            }

            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended).IsFalse();
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsEqualTo("op-1");
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
        }
    }

    /// <summary>Notifying without an active scope is a no-op.</summary>
    [Test]
    public async Task NotifyWithoutScopeIsNoOp()
    {
        RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();

        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsNull();
    }

    /// <summary>Stamping is tracked per scope across nesting.</summary>
    [Test]
    public async Task StampingTrackedPerScope()
    {
        var outer = new object();
        var inner = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(outer, "op-outer", "fp-outer");
        try
        {
            RpcMutationIdempotencyExecutionAmbient.Activate(inner, "op-inner", "fp-inner");
            try
            {
                _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(inner)).IsFalse();
                RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
                _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(inner)).IsTrue();
                _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(outer)).IsFalse();
                _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(new object())).IsFalse();
            }
            finally
            {
                RpcMutationIdempotencyExecutionAmbient.Deactivate(inner);
            }

            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsEqualTo("op-outer");
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(outer);
        }
    }
}
