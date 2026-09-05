using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Rest;

[Immutable]
internal sealed class ErrorResponse
{
    [JsonConstructor]
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
