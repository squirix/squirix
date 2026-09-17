using System;
using System.Net.Http;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Cluster mTLS startup overrides and outbound handler wiring for a test node.</summary>
/// <param name="Options">Inter-node mTLS options for host startup overrides.</param>
/// <param name="Material">Loaded certificate material backing the options.</param>
/// <param name="PeerHandlerFactory">Per-peer outbound handler factory, or <see langword="null" /> for default wiring.</param>
[Immutable]
internal readonly record struct NodeMtlsStartup(MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory);
