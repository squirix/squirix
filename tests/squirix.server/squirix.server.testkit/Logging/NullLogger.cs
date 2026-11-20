using Squirix.Server.Logging;

namespace Squirix.Server.TestKit.Logging;

internal static class NullLogger
{
    internal static ILogger Instance => NullLogger<object>.Instance;
}
