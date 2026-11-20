using Squirix.Server.Logging;

namespace Squirix.Server.TestKit.Logging;

internal sealed class NullLoggerFactory : ILoggerFactory
{
    internal static readonly NullLoggerFactory Instance = new();

    private NullLoggerFactory()
    {
    }

    public ILogger CreateLogger(string categoryName) => NullLogger.Instance;

    public void Dispose()
    {
    }
}
