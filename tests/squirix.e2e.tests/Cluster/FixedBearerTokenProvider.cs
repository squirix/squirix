using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2ETests.Cluster;

/// <summary>Provides a fixed bearer token without a capturing lambda.</summary>
internal sealed class FixedBearerTokenProvider
{
    private readonly string _token;

    internal FixedBearerTokenProvider(string token)
    {
        _token = token;
        ProvideAsync = ProvideCoreAsync;
    }

    internal Func<CancellationToken, ValueTask<string>> ProvideAsync { get; }

    private ValueTask<string> ProvideCoreAsync(CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return new ValueTask<string>(_token);
    }
}
