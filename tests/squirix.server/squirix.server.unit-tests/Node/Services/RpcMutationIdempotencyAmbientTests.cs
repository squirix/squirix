using Squirix.Server.Attributes;
using Squirix.Server.Runtime;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for the idempotency execution ambient stack.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyAmbientTests
{
    /// <summary>Deactivating a foreign scope leaves the active scope intact.</summary>
    [Fact]
    public void DeactivateMismatchKeepsScope()
    {
        var active = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(active, "op-1");
        try
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(new object());
            Assert.True(RpcMutationIdempotencyExecutionAmbient.IsDeferred);
            Assert.Equal("op-1", RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue);
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(active);
        }

        Assert.False(RpcMutationIdempotencyExecutionAmbient.IsDeferred);
    }

    /// <summary>Notifying without an active scope is a no-op.</summary>
    [Fact]
    public void NotifyWithoutScopeIsNoOp()
    {
        RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();

        Assert.False(RpcMutationIdempotencyExecutionAmbient.IsDeferred);
    }

    /// <summary>Stamping is tracked per scope across nesting.</summary>
    [Fact]
    public void StampingTrackedPerScope()
    {
        var outer = new object();
        var inner = new object();
        RpcMutationIdempotencyExecutionAmbient.Activate(outer, "op-outer");
        try
        {
            RpcMutationIdempotencyExecutionAmbient.Activate(inner, "op-inner");
            try
            {
                Assert.False(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(inner));
                RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
                Assert.True(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(inner));
                Assert.False(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(outer));
                Assert.False(RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(new object()));
            }
            finally
            {
                RpcMutationIdempotencyExecutionAmbient.Deactivate(inner);
            }

            Assert.Equal("op-outer", RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue);
        }
        finally
        {
            RpcMutationIdempotencyExecutionAmbient.Deactivate(outer);
        }
    }
}
