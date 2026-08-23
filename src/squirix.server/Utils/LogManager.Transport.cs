using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Inter-node gRPC client pool drain and disposal logs.</summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 2001, Level = LogLevel.Debug, Message = "Failed to dispose server call policy for node {NodeId} during pool drain")]
    internal static partial void ClientPoolPolicyDisposeFailed(ILogger logger, Exception exception, string nodeId);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Debug, Message = "Failed to dispose gRPC channel for node {NodeId} during pool drain")]
    internal static partial void ClientPoolChannelDisposeFailed(ILogger logger, Exception exception, string nodeId);
}
