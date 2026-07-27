using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Executes minimal ASP.NET Core <see cref="IResult" /> instances in unit tests.</summary>
internal static class HttpResultTestKit
{
    /// <summary>Executes <paramref name="result" /> and returns the HTTP status code.</summary>
    /// <param name="result">The result to execute.</param>
    /// <param name="cancellationToken">Cancellation token for result execution.</param>
    /// <returns>The response status code.</returns>
    internal static async Task<int> ExecuteStatusAsync(IResult result, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var context = CreateContext();
        await result.ExecuteAsync(context).ConfigureAwait(false);
        return context.Response.StatusCode;
    }

    /// <summary>Executes <paramref name="result" /> and returns the status code plus parsed JSON body.</summary>
    /// <param name="result">The result to execute.</param>
    /// <param name="cancellationToken">Cancellation token for result execution and JSON parsing.</param>
    /// <returns>The response status code and parsed JSON document (caller owns disposal).</returns>
    internal static async Task<(int Status, JsonDocument Payload)> ExecuteJsonAsync(
        IResult result,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var context = CreateContext();
        await result.ExecuteAsync(context).ConfigureAwait(false);
        context.Response.Body.Position = 0;
        var payload = await JsonDocument.ParseAsync(context.Response.Body, cancellationToken: cancellationToken).ConfigureAwait(false);
        return (context.Response.StatusCode, payload);
    }

    private static DefaultHttpContext CreateContext() => new()
    {
        Response =
        {
            Body = new MemoryStream(),
        },
        RequestServices = new ServiceCollection().AddLogging().BuildServiceProvider(),
    };
}
