using System;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Logger factory handing out one logger for every category; holds nothing that needs disposal.</summary>
[Immutable]
internal sealed class FixedLoggerFactory : ILoggerFactory
{
    private readonly ILogger _logger;

    /// <summary>Initializes a new instance of the <see cref="FixedLoggerFactory" /> class.</summary>
    /// <param name="logger">The logger returned for every category.</param>
    internal FixedLoggerFactory(ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <inheritdoc />
    public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => _logger;

    /// <inheritdoc />
    public void Dispose()
    {
    }
}
