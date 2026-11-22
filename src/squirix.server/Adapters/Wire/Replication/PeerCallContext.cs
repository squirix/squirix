using System.Security.Cryptography.X509Certificates;
using System.Threading;

namespace Squirix.Server.Adapters.Wire.Replication;

/// <summary>Connection facts established by the internal listener before peer command dispatch.</summary>
internal sealed class PeerCallContext
{
    internal X509Certificate2? ClientCertificate { get; init; }

    internal bool InternalListener { get; init; }

    internal int LocalPort { get; init; }

    internal CancellationToken RequestAborted { get; init; }
}
