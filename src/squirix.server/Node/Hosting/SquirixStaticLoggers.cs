using System;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Binds the static, best-effort diagnostic loggers to the host's logging pipeline.</summary>
internal static class SquirixStaticLoggers
{
    /// <summary>Assigns concrete loggers to every static diagnostic sink so suppressed-exception paths are observable.</summary>
    /// <param name="factory">The host logging factory.</param>
    public static void Configure(ILoggerFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);

        LogManager.Configure(factory);
        BackpressureMetrics.Logger = factory.CreateLogger("Squirix.Server.Node.Observability.BackpressureMetrics");
        Configurator.Logger = factory.CreateLogger("Squirix.Server.Configurator");
        DirectoryEx.Logger = factory.CreateLogger("Squirix.Server.Utils.DirectoryEx");
        DirectorySymlinkGuard.Logger = factory.CreateLogger("Squirix.Server.Utils.DirectorySymlinkGuard");
    }
}
