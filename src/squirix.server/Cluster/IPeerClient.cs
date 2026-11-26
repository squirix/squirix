using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Cluster;

/// <summary>Sends one binary operation to a configured peer.</summary>
internal interface IPeerClient
{
    Task<T> SendAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
        where T : WireMessage;

    /// <summary>Sends a ballot with a separate reply budget when the transport can distinguish connection establishment.</summary>
    /// <typeparam name="T">The reply message type.</typeparam>
    /// <param name="operation">The ballot operation.</param>
    /// <param name="request">The ballot request.</param>
    /// <param name="parser">The parser for the reply.</param>
    /// <param name="replyBudget">The duration and clock for the ballot exchange.</param>
    /// <param name="cancellationToken">The token for the entire operation.</param>
    /// <returns>The peer's reply.</returns>
    async Task<T> SendElectionAsync<T>(
        PeerOperation operation,
        WireMessage request,
        WireParser<T> parser,
        (TimeSpan Timeout, TimeProvider Clock) replyBudget,
        CancellationToken cancellationToken)
        where T : WireMessage
    {
        using var timeout = new CancellationTokenSource(replyBudget.Timeout, replyBudget.Clock);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        return await SendAsync(operation, request, parser, linked.Token).ConfigureAwait(false);
    }
}
