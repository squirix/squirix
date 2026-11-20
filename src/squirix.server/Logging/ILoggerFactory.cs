using System;

namespace Squirix.Server.Logging;

/// <summary>Resolves category loggers for a node instance.</summary>
internal interface ILoggerFactory : IDisposable
{
    ILogger<T> CreateLogger<T>() => new CategoryLogger<T>(CreateLogger(ILogger<T>.Name));

    ILogger CreateLogger(string categoryName);
}
