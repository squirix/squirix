namespace Squirix.Server.Host;

internal static class SquirixServerSettings
{
    internal static SquirixServerOptions Load(string path) => SquirixServerConfiguration.LoadFromFile(path);
}
