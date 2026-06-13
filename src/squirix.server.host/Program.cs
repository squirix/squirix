using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;

namespace Squirix.Server.Host;

[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Entry point type is already internal; analyzer still reports this file.")]
internal static class Program
{
    private static Task<int> Main(string[] args) => SquirixServerProcess.RunAsync(args);
}
