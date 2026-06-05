namespace Squirix.Server;

internal static class SquirixServerOptionsValidator
{
    public static void Validate(SquirixServerOptions options) => ClusterTopologyValidator.Validate(options);
}
