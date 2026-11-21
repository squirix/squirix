using Squirix.Server.Logging;

namespace Squirix.Server.TestKit.Logging;

internal static class NullLogger
{
    internal static IServerLogger Instance => NullLogger<object>.Instance;
}
