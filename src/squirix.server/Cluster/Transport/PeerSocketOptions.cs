using System;
using System.Net;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Transport;

internal sealed record PeerSocketOptions(
    SslClientAuthenticationOptions Tls,
    Func<CancellationToken, ValueTask<EndPoint>>? ResolveEndpoint);
