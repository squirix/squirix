using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
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

    /// <summary>A scope begun inside a scope-less suspension stamps while active, and ending it returns to the suspension, which ends with its disposal.</summary>
    [Test]
    public async Task ScopeBegunInsideScopelessSuspension()
    {
        var scope = new object();
        string? insideOperationId;
        bool suspendedAfterScope;
        using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
        {
            RpcMutationIdempotencyExecutionAmbient.Activate(scope, "op", "fp");
            insideOperationId = RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue;
            RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
            suspendedAfterScope = RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended;
        }

        _ = await Assert.That(insideOperationId).IsEqualTo("op");
        _ = await Assert.That(suspendedAfterScope).IsTrue();
        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended).IsFalse();
        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsNull();
    }

    /// <summary>An exception thrown inside a scope-less suspension still restores the empty state.</summary>
    [Test]
    public async Task ThrowInsideSuspensionRestoresFrame()
    {
        try
        {
            using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
                throw new InvalidOperationException("Injected failure inside the suspension.");
        }
        catch (InvalidOperationException)
        {
            // The suspension was disposed by the unwinding using.
        }

        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended).IsFalse();
        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsNull();
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
        JournalMutationContext.Current?.NotifyMutationStamped();

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
                JournalMutationContext.Current?.NotifyMutationStamped();
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

    /// <summary>A journal stamp marks the RPC operation and notifies its reservation through the storage-owned port.</summary>
    [Test]
    public async Task JournalStampNotifiesRpcScope()
    {
        var scope = new StampListener();
        RpcMutationIdempotencyExecutionAmbient.Activate(scope, "operation", "fingerprint");
        try
        {
            var context = JournalMutationContext.Current!;
            context.NotifyMutationStamped();

            _ = await Assert.That(context.OperationId).IsEqualTo("operation");
            _ = await Assert.That(context.Fingerprint).IsEqualTo("fingerprint");
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(scope)).IsTrue();
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasTakenEffect(scope)).IsTrue();
            _ = await Assert.That(scope.Stamps).IsEqualTo(1);
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
        }
    }

    /// <summary>Suspension hides the stamp but shares the operation's accepted effects and outcome guard.</summary>
    [Test]
    public async Task SuspendedContextSharesOperationState()
    {
        var scope = new StampListener();
        RpcMutationIdempotencyExecutionAmbient.Activate(scope, "operation", "fingerprint");
        try
        {
            var original = JournalMutationContext.Current;
            RpcMutationIdempotencyExecutionAmbient.NotifyOutcomeAppended();
            using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
            {
                var suspended = JournalMutationContext.Current!;
                _ = await Assert.That(suspended.OperationId).IsNull();
                _ = await Assert.That(suspended.Fingerprint).IsNull();
                _ = await Assert.That(suspended.HasAppendedOutcome).IsTrue();
                suspended.NotifyMutationApplied();
            }

            _ = await Assert.That(JournalMutationContext.Current).IsSameReferenceAs(original);
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasTakenEffect(scope)).IsTrue();
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(scope)).IsFalse();
            _ = await Assert.That(scope.Stamps).IsEqualTo(0);
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
        }
    }

    /// <summary>A nested asynchronous operation restores its caller's journal context without stamping the caller.</summary>
    [Test]
    public async Task NestedAsyncJournalContextRestoresCaller()
    {
        var outer = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(outer, "outer", "outer-fingerprint");
        try
        {
            var original = JournalMutationContext.Current;
            await RunInnerScopeAsync();

            _ = await Assert.That(JournalMutationContext.Current).IsSameReferenceAs(original);
            _ = await Assert.That(JournalMutationContext.Current!.OperationId).IsEqualTo("outer");
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(outer)).IsFalse();
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(outer);
        }
    }

    /// <summary>RPC activation and suspension restore a journal context supplied by another caller.</summary>
    [Test]
    public async Task RpcScopeRestoresForeignJournalContext()
    {
        var foreign = new IJournalMutationContextCreateExpectations().Instance();
        var previous = JournalMutationContext.Current;
        JournalMutationContext.Current = foreign;
        try
        {
            var scope = new object();
            RpcMutationIdempotencyExecutionAmbient.Activate(scope, "operation", "fingerprint");
            try
            {
                _ = await Assert.That(JournalMutationContext.Current.OperationId).IsEqualTo("operation");
            }
            finally
            {
                RpcMutationIdempotencyExecutionAmbient.Deactivate(scope);
            }

            _ = await Assert.That(JournalMutationContext.Current).IsSameReferenceAs(foreign);
            using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
                _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsStampingSuspended).IsTrue();
            _ = await Assert.That(JournalMutationContext.Current).IsSameReferenceAs(foreign);
        }
        finally
        {
            JournalMutationContext.Current = previous;
        }
    }

    private static async Task RunInnerScopeAsync()
    {
        var inner = new StampListener();
        RpcMutationIdempotencyExecutionAmbient.Activate(inner, "inner", "inner-fingerprint");
        try
        {
            await Task.Yield();
            var context = JournalMutationContext.Current!;
            context.NotifyMutationStamped();
            _ = await Assert.That(context.OperationId).IsEqualTo("inner");
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(inner)).IsTrue();
            _ = await Assert.That(inner.Stamps).IsEqualTo(1);
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(inner);
        }
    }

    private sealed class StampListener : IRpcMutationStampListener
    {
        internal int Stamps { get; private set; }

        public void OnMutationStamped() => Stamps++;
    }
}
