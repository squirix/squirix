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
        RpcMutationIdempotencyExecutionAmbient.Activate(active, "op-1");
        try
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(new object());
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsDeferred).IsTrue();
            _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue).IsEqualTo("op-1");
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(active);
        }

        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsDeferred).IsFalse();
    }

    /// <summary>Notifying without an active scope is a no-op.</summary>
    [Test]
    public async Task NotifyWithoutScopeIsNoOp()
    {
        RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();

        _ = await Assert.That(RpcMutationIdempotencyExecutionAmbient.IsDeferred).IsFalse();
    }

    /// <summary>Stamping is tracked per scope across nesting.</summary>
    [Test]
    public async Task StampingTrackedPerScope()
    {
        var outer = new object();
        var inner = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(outer, "op-outer");
        try
        {
            RpcMutationIdempotencyExecutionAmbient.Activate(inner, "op-inner");
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
