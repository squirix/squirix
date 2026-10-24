using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Routing.Matching;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>
/// Drops endpoints marked with <see cref="InternalListenerEndpointMetadata" /> unless the request arrived on the internal listener port with a peer
/// certificate. The request Host header is attacker-controlled and plays no part in the decision, so every other request falls through to the
/// externally authorized mapping.
/// </summary>
internal sealed class InternalListenerMatcherPolicy : MatcherPolicy, IEndpointSelectorPolicy
{
    private const int PolicyOrder = -200;

    /// <inheritdoc />
    public override int Order => PolicyOrder;

    /// <inheritdoc />
    public bool AppliesToEndpoints(IReadOnlyList<Microsoft.AspNetCore.Http.Endpoint> endpoints)
    {
        for (var i = 0; i < endpoints.Count; i++)
        {
            if (endpoints[i].Metadata.GetMetadata<InternalListenerEndpointMetadata>() != null)
                return true;
        }

        return false;
    }

    /// <inheritdoc />
    public Task ApplyAsync(HttpContext httpContext, CandidateSet candidates)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(candidates);

        for (var i = 0; i < candidates.Count; i++)
        {
            var metadata = candidates[i].Endpoint.Metadata.GetMetadata<InternalListenerEndpointMetadata>();
            if (metadata == null)
                continue;

            var internalConnection = InternalListenerConnection.ArrivedWithPeerCertificate(httpContext, metadata.InternalListenPort);
            if (internalConnection != metadata.InternalOnly)
                candidates.SetValidity(i, false);
        }

        return Task.CompletedTask;
    }
}
