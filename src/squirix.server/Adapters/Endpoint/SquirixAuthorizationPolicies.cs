namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Authorization policy names applied by Squirix endpoint mapping.</summary>
internal static class SquirixAuthorizationPolicies
{
    /// <summary>JWT bearer authentication policy for public cache gRPC and remote health/metrics HTTP scrapes.</summary>
    internal const string JwtBearer = "JwtBearer";
}
