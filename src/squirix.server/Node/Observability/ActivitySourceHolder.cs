using System.Diagnostics;

namespace Squirix.Server.Node.Observability;

internal static class ActivitySourceHolder
{
    public static readonly ActivitySource Squirix = new("Squirix");

    public static Activity? StartInternal(string name) => Squirix.StartActivity(name, ActivityKind.Internal, (ActivityContext)default);
}
