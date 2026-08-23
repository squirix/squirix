using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Squirix.Server.Utils;

/// <summary>Host logging wiring for <see cref="LogManager" />.</summary>
internal static partial class LogManager
{
    private static ILoggerFactory? HostFactory { get; set; }

    internal static void Configure(ILoggerFactory factory) => HostFactory = factory ?? throw new ArgumentNullException(nameof(factory));

    internal static ILogger<T> GetLogger<T>()
        where T : class => HostFactory?.CreateLogger<T>() ?? NullLogger<T>.Instance;

    internal static ILogger GetLogger(string categoryName) => HostFactory?.CreateLogger(categoryName) ?? NullLogger.Instance;
}
