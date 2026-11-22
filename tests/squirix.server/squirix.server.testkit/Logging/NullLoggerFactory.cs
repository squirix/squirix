using Squirix.Server.Logging;

namespace Squirix.Server.TestKit.Logging;

internal sealed class NullLoggerFactory : IServerLoggerFactory
{
    internal static readonly NullLoggerFactory Instance = new();

    private NullLoggerFactory()
    {
    }

    public IServerLogger CreateLogger(string categoryName) => NullLogger.Instance;

    public void Dispose()
    {
    }
}
