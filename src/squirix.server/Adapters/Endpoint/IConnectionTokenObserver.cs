using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint;

internal interface IConnectionTokenObserver
{
    ValueTask<ClaimsPrincipal?> OnValidatedAsync(ClaimsPrincipal principal, IServiceProvider services, BinaryConnection? connection, CancellationToken cancellationToken);
}
