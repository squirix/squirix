using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Squirix.Server.TestKit.Diagnostics;

/// <summary>Best-effort logging for exceptions intentionally suppressed in test helpers.</summary>
internal static class TestLog
{
    private static readonly Action<ILogger, string, Exception> LogSuppressed = LoggerMessage.Define<string>(LogLevel.Debug, new EventId(1, "TestSuppressed"), "{Context}");

    public static void Suppressed(string context, Exception exception) => LogSuppressed(NullLogger.Instance, context, exception);
}
