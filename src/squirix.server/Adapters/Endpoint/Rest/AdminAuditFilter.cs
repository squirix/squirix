using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Adapters.Endpoint.Rest;

/// <summary>
/// Minimal API endpoint filter that emits audit logs for administrative REST calls.
/// </summary>
internal sealed class AdminAuditFilter : IEndpointFilter
{
    private static readonly Action<ILogger, string, string, string, int, Exception?> AdminActionExecutedMessage = LoggerMessage.Define<string, string, string, int>(
        LogLevel.Information,
        new EventId(1000, nameof(AdminActionExecutedMessage)),
        "Admin action {Action} executed by {User} from {RemoteAddress} with status {StatusCode}");

    private static readonly Action<ILogger, string, string, string, Exception?> AdminActionFailedMessage = LoggerMessage.Define<string, string, string>(
        LogLevel.Warning,
        new EventId(1001, nameof(AdminActionFailedMessage)),
        "Admin action {Action} failed for {User} from {RemoteAddress}");

    private readonly ILogger _logger;

    private readonly AdminAuditSink _sink;

    public AdminAuditFilter(AdminAuditSink sink, ILogger logger)
    {
        _sink = sink;
        _logger = logger;
    }

    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var http = context.HttpContext;
        var request = http.Request;
        var action = $"{request.Method} {request.Path}";
        var remote = http.Connection.RemoteIpAddress?.ToString() ?? "unknown";
        var user = http.User.Identity?.IsAuthenticated == true ? http.User.Identity?.Name ?? "authenticated-user" : "anonymous";

        try
        {
            var result = await next(context);
            var statusCode = ExtractStatusCode(result, http.Response);
            AdminActionExecutedMessage(_logger, action, user, remote, statusCode, null);
            _sink.Record(new AdminAuditEvent(DateTime.UtcNow, action, user, remote, statusCode, null));
            return result;
        }
        catch (Exception ex)
        {
            AdminActionFailedMessage(_logger, action, user, remote, ex);
            _sink.Record(new AdminAuditEvent(DateTime.UtcNow, action, user, remote, StatusCodes.Status500InternalServerError, ex.Message));
            throw;
        }
    }

    private static int ExtractStatusCode(object? result, HttpResponse response)
    {
        return result switch
        {
            IStatusCodeHttpResult { StatusCode: not null } statusResult => statusResult.StatusCode.Value,
            StatusCodeHttpResult specificStatus => specificStatus.StatusCode,
            _ => response.StatusCode,
        };
    }
}
