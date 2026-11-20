using System;
using System.Threading.Tasks;

namespace Squirix.Server.Composition;

/// <summary>Optional per-request services supplied by an endpoint hook.</summary>
/// <param name="ServiceProvider">Services visible to the request.</param>
/// <param name="Lifetime">Optional owner of request-scoped components.</param>
internal readonly record struct NodeRequestScope(IServiceProvider ServiceProvider, IAsyncDisposable? Lifetime) : IAsyncDisposable
{
    public ValueTask DisposeAsync() => Lifetime?.DisposeAsync() ?? ValueTask.CompletedTask;
}
