using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Squirix.Server.Utils;

/// <summary>Host logging wiring for <see cref="LogManager" />.</summary>
internal static partial class LogManager
{
    private static ILoggerFactory? _hostFactory;

    internal static void Configure(ILoggerFactory factory) => _hostFactory = factory ?? throw new ArgumentNullException(nameof(factory));

    internal static ILogger<T> GetLogger<T>()
        where T : class => _hostFactory?.CreateLogger<T>() ?? NullLogger<T>.Instance;
}
