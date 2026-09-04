using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Squirix.Server.Utils;

/// <summary>Host logging wiring for <see cref="LogManager" />.</summary>
internal static partial class LogManager
{
    private static ILoggerFactory? HostFactory { get; set; }

    internal static void Configure(ILoggerFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        HostFactory = factory;
    }

    internal static ILogger<T> GetLogger<T>()
        where T : class
    {
        var factory = HostFactory;
        if (factory == null)
            return NullLogger<T>.Instance;

        try
        {
            return factory.CreateLogger<T>();
        }
        catch (ObjectDisposedException)
        {
            // The host that supplied the factory has been torn down; diagnostics are best-effort.
            return NullLogger<T>.Instance;
        }
    }

    internal static ILogger GetLogger(string categoryName)
    {
        var factory = HostFactory;
        if (factory == null)
            return NullLogger.Instance;

        try
        {
            return factory.CreateLogger(categoryName);
        }
        catch (ObjectDisposedException)
        {
            // The host that supplied the factory has been torn down; diagnostics are best-effort.
            return NullLogger.Instance;
        }
    }
}
