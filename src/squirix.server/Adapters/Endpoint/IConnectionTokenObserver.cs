using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Observes the validated principal during authentication.</summary>
/// <remarks>Return promptly and await I/O asynchronously so authentication can yield while the observer waits.</remarks>
internal interface IConnectionTokenObserver
{
    ValueTask<ClaimsPrincipal?> OnValidatedAsync(ClaimsPrincipal principal, IServiceProvider services, BinaryConnection? connection, CancellationToken cancellationToken);
}
