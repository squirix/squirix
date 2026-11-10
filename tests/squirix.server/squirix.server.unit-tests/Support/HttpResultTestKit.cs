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
    /// <summary>Executes <paramref name="result" /> and returns the status code plus parsed JSON body.</summary>
    /// <param name="result">The result to execute.</param>
    /// <param name="cancellationToken">Cancellation token for result execution and JSON parsing.</param>
    /// <returns>The response status code and parsed JSON document (caller owns disposal).</returns>
    internal static async Task<(int Status, JsonDocument Payload)> ExecuteJsonAsync(
        IResult result,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await using var body = new MemoryStream();
        await using var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        var context = new DefaultHttpContext
        {
            RequestServices = services,
            Response =
            {
                Body = body,
            },
        };
        await result.ExecuteAsync(context).ConfigureAwait(false);
        body.Position = 0;
        var payload = await JsonDocument.ParseAsync(body, cancellationToken: cancellationToken).ConfigureAwait(false);
        return (context.Response.StatusCode, payload);
    }
}
