using System;

namespace Squirix.Server.Logging;

/// <summary>Resolves category loggers for a node instance.</summary>
internal interface IServerLoggerFactory : IDisposable
{
    IServerLogger<T> CreateLogger<T>() => new CategoryLogger<T>(CreateLogger(IServerLogger<T>.Name));

    IServerLogger CreateLogger(string categoryName);
}
