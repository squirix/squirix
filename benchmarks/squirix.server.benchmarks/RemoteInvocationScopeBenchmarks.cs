using BenchmarkDotNet.Attributes;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of classifying a public or forwarded RPC, including nested invocation scopes.</summary>
[MemoryDiagnoser]
public class RemoteInvocationScopeBenchmarks
{
    /// <summary>Gets or sets a value indicating whether the inner request is an internal owner invocation.</summary>
    [Params(false, true)]
    public bool InternalOwner { get; set; }

    /// <summary>Enters and leaves an invocation classification from an ordinary public context.</summary>
    /// <returns>The classification observed inside the scope.</returns>
    [Benchmark]
    public bool EnterAndLeave()
    {
        using var scope = RemoteInvocationContext.EnterRemoteInvocation(InternalOwner);
        return RemoteInvocationContext.IsInternalOwnerInvocation;
    }

    /// <summary>Enters an invocation classification inside an internal owner invocation and restores both scopes.</summary>
    /// <returns>The classification observed inside the nested scope.</returns>
    [Benchmark]
    public bool EnterNested()
    {
        using var outer = RemoteInvocationContext.EnterRemoteInvocation(true);
        using var inner = RemoteInvocationContext.EnterRemoteInvocation(InternalOwner);
        return RemoteInvocationContext.IsInternalOwnerInvocation;
    }
}
