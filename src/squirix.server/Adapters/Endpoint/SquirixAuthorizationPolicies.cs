namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Authorization policy names applied by Squirix endpoint mapping.</summary>
internal static class SquirixAuthorizationPolicies
{
    /// <summary>JWT bearer authentication policy for public cache RPC and REST endpoints.</summary>
    internal const string JwtBearer = "JwtBearer";
}
