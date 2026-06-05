using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;

namespace Squirix.Server;

/// <summary>
/// Lifetime handle for a started squirix server application.
/// </summary>
internal sealed class SquirixServerApplicationHandle : IAsyncDisposable
{
    private readonly WebApplication _app;

    internal SquirixServerApplicationHandle(WebApplication app)
    {
        _app = app ?? throw new ArgumentNullException(nameof(app));
    }

    /// <summary>
    /// Ends the server application and releases the owned ASP.NET Core host.
    /// </summary>
    /// <returns>A task that completes when the application is disposed.</returns>
    public ValueTask DisposeAsync() => _app.DisposeAsync();
}
