using System;

namespace Squirix.Server.Adapters.Endpoint.Rest;

internal sealed record AdminAuditEvent(DateTime TimestampUtc, string Action, string User, string RemoteAddress, int StatusCode, string? Error);
