using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Configures optional package extensions for a Squirix server host.</summary>
public sealed class ExtensionOptions
{
    /// <summary>Gets or sets a callback that registers extension services after core services are registered.</summary>
    public Action<IServiceCollection>? ConfigureServices { get; set; }

    /// <summary>Gets or sets a callback that decorates the hosted basic cache pipeline.</summary>
    /// <remarks>A decorator sees every attempt of a mutating call, including a client retry of an operation that already committed, so it must be idempotent per operation id.</remarks>
    public Func<IServiceProvider, ISquirixServerCachePipeline, ISquirixServerCachePipeline>? DecorateCachePipeline { get; set; }

    /// <summary>Gets or sets a callback that maps extension endpoints after core endpoints are mapped.</summary>
    public Action<WebApplication>? MapEndpoints { get; set; }

    /// <summary>Gets or sets a callback that maps extension endpoints after core endpoints are mapped and receives whether host authentication is enabled.</summary>
    public Action<WebApplication, bool>? MapEndpointsWithAuthorization { get; set; }
}
