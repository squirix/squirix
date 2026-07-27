using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Minimal <see cref="ServerCallContext" /> for interceptor unit tests.</summary>
internal sealed class TestServerCallContext : ServerCallContext
{
    /// <summary>Initializes a new instance of the <see cref="TestServerCallContext" /> class.</summary>
    /// <param name="headers">Optional request headers; empty when omitted.</param>
    internal TestServerCallContext(Metadata? headers = null)
    {
        RequestHeadersCore = headers ?? [];
    }

    /// <inheritdoc />
    protected override AuthContext AuthContextCore => new(null, []);

    /// <inheritdoc />
    protected override CancellationToken CancellationTokenCore => CancellationToken.None;

    /// <inheritdoc />
    protected override DateTime DeadlineCore => DateTime.MaxValue;

    /// <inheritdoc />
    protected override string HostCore => "localhost";

    /// <inheritdoc />
    protected override string MethodCore => "/Test.Test/Unary";

    /// <inheritdoc />
    protected override string PeerCore => "ipv4:127.0.0.1:5001";

    /// <inheritdoc />
    protected override Metadata RequestHeadersCore { get; }

    /// <inheritdoc />
    protected override Metadata ResponseTrailersCore => [];

    /// <inheritdoc />
    protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

    /// <inheritdoc />
    protected override WriteOptions? WriteOptionsCore { get; set; }

    /// <inheritdoc />
    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();

    /// <inheritdoc />
    protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
}
