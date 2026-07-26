using System.Diagnostics;

namespace Squirix.Server.Node.Observability;

internal static class ActivitySourceHolder
{
    internal const string SourceName = "Squirix";

    private static readonly ActivitySource Source = new(SourceName);

    internal static Activity? StartClient(string name) => Source.StartActivity(name, ActivityKind.Client);

    internal static Activity? StartInternal(string name) => Source.StartActivity(name);

    internal static Activity? StartServer(string name, in ActivityContext parentContext = default) => parentContext != default
        ? Source.StartActivity(name, ActivityKind.Server, parentContext) : Source.StartActivity(name, ActivityKind.Server);
}
