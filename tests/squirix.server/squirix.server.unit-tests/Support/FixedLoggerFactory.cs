using System;
using Squirix.Server.Attributes;
using Squirix.Server.Logging;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Logger factory handing out one logger for every category; holds nothing that needs disposal.</summary>
[Immutable]
internal sealed class FixedLoggerFactory : IServerLoggerFactory
{
    private readonly IServerLogger _logger;

    /// <summary>Initializes a new instance of the <see cref="FixedLoggerFactory" /> class.</summary>
    /// <param name="logger">The logger returned for every category.</param>
    internal FixedLoggerFactory(IServerLogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <inheritdoc />
    public IServerLogger CreateLogger(string categoryName) => _logger;

    /// <inheritdoc />
    public void Dispose()
    {
    }
}
