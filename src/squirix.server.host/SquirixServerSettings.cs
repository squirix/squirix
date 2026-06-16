using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Host;

internal static class SquirixServerSettings
{
    internal static Task<SquirixServerOptions> LoadAsync(string path, CancellationToken cancellationToken = default) =>
        SquirixServerConfiguration.LoadFromFileAsync(path, cancellationToken);
}
