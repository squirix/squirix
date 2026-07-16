using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal sealed class ErrorResponse
{
    internal ErrorResponse(string error, string code, string? detail)
    {
        Error = error;
        Code = code;
        Detail = detail;
    }

    [JsonInclude]
    internal string Code { get; }

    [JsonInclude]
    internal string? Detail { get; }

    [JsonInclude]
    internal string Error { get; }
}
