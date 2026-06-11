namespace Squirix.Server.SmokeTests;

/// <summary>
/// Inventory of JWT-gated surfaces in core <c>Squirix.Server</c> and the smoke tests that assert auth contracts.
/// </summary>
/// <remarks>
/// <para>Sources: <c>SquirixEndpointMapping</c>, <c>HttpEndpointEx</c>, <c>SquirixMetricsConnectionSecurity</c>.</para>
/// <list type="table">
/// <listheader>
/// <term>Surface</term>
/// <description>Fail-closed smoke coverage</description>
/// </listheader>
/// <item>
/// <term>gRPC <c>SquirixCacheService</c> (all RPCs share <c>JwtBearer</c> policy)</term>
/// <description><see cref="Grpc.GrpcAuthSmokeTests" /></description>
/// </item>
/// <item>
/// <term><c>/metrics</c> from non-loopback clients</term>
/// <description><see cref="Observability.MetricsAuthSmokeTests" /></description>
/// </item>
/// <item>
/// <term><c>/metrics</c> from loopback</term>
/// <description>Anonymous allowed — positive path in <see cref="Observability.MetricsAuthSmokeTests" /></description>
/// </item>
/// <item>
/// <term><c>/health/*</c></term>
/// <description>Public when auth is enabled — <see cref="Health.HealthProbeSmokeTests" /></description>
/// </item>
/// </list>
/// </remarks>
internal static class AuthProtectedSurfaceInventory
{
}
