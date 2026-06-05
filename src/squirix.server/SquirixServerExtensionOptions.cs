using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server;

/// <summary>
/// Configures optional package extensions for a Squirix server host.
/// </summary>
public sealed class SquirixServerExtensionOptions
{
    /// <summary>
    /// Gets or sets a callback that registers extension services after core services are registered.
    /// </summary>
    public Action<IServiceCollection>? ConfigureServices { get; set; }

    /// <summary>
    /// Gets or sets a callback that decorates the hosted basic cache pipeline.
    /// </summary>
    public Func<IServiceProvider, ISquirixServerCachePipeline<object?>, ISquirixServerCachePipeline<object?>>? DecorateCachePipeline { get; set; }

    /// <summary>
    /// Gets or sets a callback that maps extension endpoints after core endpoints are mapped.
    /// </summary>
    public Action<WebApplication>? MapEndpoints { get; set; }

    /// <summary>
    /// Gets or sets a callback that maps extension endpoints after core endpoints are mapped and receives whether host authentication is enabled.
    /// </summary>
    public Action<WebApplication, bool>? MapEndpointsWithAuthorization { get; set; }
}
