using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint;

internal interface IJwtMetadataSource
{
    bool UseLastKnownGoodConfiguration { get; }

    TimeSpan LastKnownGoodLifetime { get; }

    Task<JwtMetadata> GetConfigurationAsync(CancellationToken cancellationToken);

    void RequestRefresh();
}
