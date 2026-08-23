using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Hosting configuration and listener lifecycle logs.</summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 2101, Level = LogLevel.Debug, Message = "TCP listener release failed for port {Port} during best-effort cleanup")]
    internal static partial void ListenerReleaseFailed(ILogger logger, Exception exception, string port);
}
