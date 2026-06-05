using System.Threading.Tasks;

namespace Squirix.Server.Host;

internal static class Program
{
    private static Task<int> Main(string[] args) => SquirixServerProcess.RunAsync(args);
}
