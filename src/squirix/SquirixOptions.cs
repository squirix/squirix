using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Serialization;

namespace Squirix;

/// <summary>
/// Provides client-side configuration for remote <see cref="SquirixClient" /> sessions.
/// </summary>
public sealed class SquirixOptions
{
    /// <summary>
    /// Gets or sets a static API key sent as the <c>x-api-key</c> header on every gRPC call.
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// Gets or sets a delegate that provides a bearer token for each gRPC call.
    /// Called before every RPC; implementations should cache tokens when appropriate.
    /// </summary>
    public Func<CancellationToken, ValueTask<string>>? BearerTokenProvider { get; set; }

    /// <summary>
    /// Gets bootstrap Squirix server endpoints used by remote clients.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     Endpoints must be interchangeable views of the same logical service (HA standby or load-balanced front door),
    ///     not independent shards. The server cluster routes keys to owners internally.
    ///     </para>
    ///     <para>
    ///     Connect succeeds when at least one endpoint is reachable (in list order).
    ///     After connect, single-RPC operations fail over to the next bootstrap endpoint on transport-level errors
    ///     (for example gRPC <c>Unavailable</c>).
    ///     </para>
    ///     <para>This is bootstrap/high-availability routing, not full cluster partition routing or consensus membership.</para>
    /// </remarks>
    public IList<string> Endpoints { get; } = [];

    /// <summary>
    /// Gets or sets an optional HTTP handler for gRPC transport.
    /// When null, the default HTTPS handler is used.
    /// </summary>
    /// <remarks>
    /// Custom handlers are useful for local loopback development with the ASP.NET Core development certificate,
    /// corporate TLS inspection, or explicit proxy configuration. The created <see cref="SquirixClient" /> session
    /// owns the handler lifetime when one is supplied.
    /// </remarks>
    public HttpMessageHandler? HttpMessageHandler { get; set; }

    /// <summary>
    /// Gets or sets the serializer implementation used by the client session created from these options.
    /// Leave null to use the default <see cref="Serialization.SystemTextJsonSerializer" /> for this client.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     The configured serializer is scoped to the created <see cref="SquirixClient" /> session and does not mutate
    ///     process-wide serializer state.
    ///     </para>
    ///     <para>
    ///     This property uses a <c>set</c> accessor (not <c>init</c>) because
    ///     <see cref="SquirixClient.ConnectAsync(Action{SquirixOptions}, CancellationToken)" /> constructs options first,
    ///     then invokes a configure delegate that assigns members after construction.
    ///     </para>
    /// </remarks>
    public ISquirixSerializer? Serializer { get; set; }
}
