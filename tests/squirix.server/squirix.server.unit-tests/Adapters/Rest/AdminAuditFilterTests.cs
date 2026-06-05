using System;
using System.Net;
using System.Security.Claims;
using System.Threading.Tasks;
using FakeItEasy;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Endpoint.Rest;
using Xunit;

namespace Squirix.Server.UnitTests.Adapters.Rest;

/// <summary>
/// Unit tests covering admin REST audit capture.
/// </summary>
public sealed class AdminAuditFilterTests
{
    /// <summary>
    /// Ensures failed admin requests are captured with the error message and failure status code.
    /// </summary>
    /// <returns>A task representing the asynchronous test flow.</returns>
    [Fact]
    public async Task InvokeAsyncRecordsFailedRequestDetails()
    {
        var sink = new AdminAuditSink();
        var filter = new AdminAuditFilter(sink, NullLogger.Instance);
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = IPAddress.Parse("198.51.100.7"),
            },
            Request =
            {
                Method = HttpMethods.Delete,
                Path = "/admin/cluster/node-1",
            },
        };

        var context = A.Fake<EndpointFilterInvocationContext>();
        _ = A.CallTo(() => context.HttpContext).Returns(http);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await filter.InvokeAsync(
            context,
            static _ => ValueTask.FromException<object?>(new InvalidOperationException("boom"))));

        var snapshot = sink.GetSnapshot();

        Assert.Equal("boom", error.Message);
        _ = Assert.Single(snapshot);
        Assert.Equal("DELETE /admin/cluster/node-1", snapshot[0].Action);
        Assert.Equal("anonymous", snapshot[0].User);
        Assert.Equal("198.51.100.7", snapshot[0].RemoteAddress);
        Assert.Equal(StatusCodes.Status500InternalServerError, snapshot[0].StatusCode);
        Assert.Equal("boom", snapshot[0].Error);
    }

    /// <summary>
    /// Ensures successful admin requests are captured with identity, address, action, and status details.
    /// </summary>
    /// <returns>A task representing the asynchronous test flow.</returns>
    [Fact]
    public async Task InvokeAsyncRecordsSuccessfulRequestDetails()
    {
        var sink = new AdminAuditSink();
        var filter = new AdminAuditFilter(sink, NullLogger.Instance);
        var http = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "alice")], "test")),
            Connection =
            {
                RemoteIpAddress = IPAddress.Parse("203.0.113.10"),
            },
            Request =
            {
                Method = HttpMethods.Post,
                Path = "/admin/storage/compact",
            },
        };

        var context = A.Fake<EndpointFilterInvocationContext>();
        _ = A.CallTo(() => context.HttpContext).Returns(http);

        var result = await filter.InvokeAsync(context, static _ => ValueTask.FromResult<object?>(TypedResults.Ok()));

        var snapshot = sink.GetSnapshot();

        _ = Assert.IsType<Ok>(result);
        _ = Assert.Single(snapshot);
        Assert.Equal("POST /admin/storage/compact", snapshot[0].Action);
        Assert.Equal("alice", snapshot[0].User);
        Assert.Equal("203.0.113.10", snapshot[0].RemoteAddress);
        Assert.Equal(StatusCodes.Status200OK, snapshot[0].StatusCode);
        Assert.Null(snapshot[0].Error);
    }
}
