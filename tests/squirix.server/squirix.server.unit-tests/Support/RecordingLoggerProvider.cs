using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Logger provider handing out one <see cref="EventRecordingLogger" /> for every category.</summary>
[Immutable]
internal sealed class RecordingLoggerProvider : ILoggerProvider
{
    private readonly EventRecordingLogger _log;

    /// <summary>Initializes a new instance of the <see cref="RecordingLoggerProvider" /> class.</summary>
    /// <param name="log">The logger returned for every category.</param>
    internal RecordingLoggerProvider(EventRecordingLogger log)
    {
        _log = log;
    }

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => _log;

    /// <inheritdoc />
    public void Dispose()
    {
    }
}
