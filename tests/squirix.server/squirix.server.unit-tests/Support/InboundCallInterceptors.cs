using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Builds inbound call interceptors for tests that run handlers outside a real gRPC pipeline.</summary>
internal static class InboundCallInterceptors
{
    private const string NodeId = "n1";

    /// <summary>Creates an interceptor for a node without internode mTLS.</summary>
    /// <returns>A new interceptor over a no-op invocation scope.</returns>
    internal static InboundCallInterceptor Create()
    {
        var scopeFactory = new IRemoteInvocationScopeFactoryCreateExpectations();
        _ = scopeFactory.Setups.EnterRemoteInvocation(Arg.Any<bool>()).Callback(static isInternalOwnerInvocation => RemoteInvocationContext.EnterRemoteInvocation(isInternalOwnerInvocation));
        return new InboundCallInterceptor(
            scopeFactory.Instance(),
            new MtlsOptions(),
            null,
            RingAgreements.Create(),
            new ServerCallCorrelation(NullLogger<ServerCallCorrelation>.Instance, NodeId));
    }
}
