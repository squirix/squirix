using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Owns the node's public TLS socket and drains it before the storage services stop.</summary>
internal sealed partial class NativeClientService : IHostedService, IAsyncDisposable
{
    private readonly IServiceProvider _services;
    private readonly TopologyOptions _topology;
    private readonly bool _mutationReplay;
    private readonly Uri? _managementUri;
    private readonly X509Certificate2? _suppliedCertificate;
    private readonly CancellationTokenSource _graceful = new();
    private readonly CancellationTokenSource _abort = new();
    private int _disposeStarted;
    private Task _run = Task.CompletedTask;
    private MtlsCertificate.Hold? _peerMaterial;
    private X509Certificate2? _certificate;

    internal NativeClientService(IServiceProvider services, TopologyOptions topology, bool mutationReplay, Uri? managementUri, X509Certificate2? suppliedCertificate)
    {
        _services = services;
        _topology = topology;
        _mutationReplay = mutationReplay;
        _managementUri = managementUri;
        _suppliedCertificate = suppliedCertificate;
    }

    internal Uri? ManagementUri { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _certificate = AcquireCertificate();
        ValidateCertificate(_certificate);
        var tls = new SslServerAuthenticationOptions { ServerCertificateContext = SslStreamCertificateContext.Create(_certificate, null, true) };
        var managementUri = _managementUri ?? ResolveManagementUri(_topology.Uri);
        _ = _services.GetService<PrometheusMetricsScraper>();
        var management = new NativeManagementEndpoint(_services, AuthenticateManagementAsync, _services.GetRequiredService<ILogger<NativeManagementEndpoint>>());
        var listeners = new List<(Socket Socket, NativeTlsListener Listener)>();
        var sessions = new List<Task>();

        try
        {
            AddListeners(_topology.Uri, socket => NativeBinaryEndpoint.CreateListener(socket, tls, _services, _mutationReplay, 1024), listeners);
            var managementIndex = listeners.Count;
            AddListeners(managementUri, socket => new NativeTlsListener(socket, tls, management.ServeAsync, _services.GetRequiredService<ILogger<NativeTlsListener>>(), 64), listeners);
            if (listeners[managementIndex].Socket.LocalEndPoint is IPEndPoint boundManagement)
                ManagementUri = new UriBuilder(managementUri) { Port = boundManagement.Port }.Uri;
            var material = _services.GetRequiredService<MtlsCertificate>();
            if (material.Enabled)
            {
                _peerMaterial = material.Retain();
                var port = _services.GetRequiredService<MtlsOptions>().InternalListenPort;
                var internalUri = new UriBuilder(_topology.Uri) { Port = port }.Uri;
                var feature = _services.GetRequiredService<FeatureState>();
                var peer = new NativePeerEndpoint(_services, port, feature.NetworkReplicationEnabled || feature.FoundationOnly);
                var verifier = new PeerHandshakeVerifier(material.PeerChains!, TimeProvider.System, _services.GetRequiredService<ILogger<PeerHandshakeVerifier>>());
                var handshake = new InboundPeerHandshake(verifier, material.NodeCertificate!, MtlsTopology.GetRemotePeerNodeIds(_topology));
                AddListeners(internalUri, socket => new NativeTlsListener(socket, handshake.CreateServerOptions, peer.ServeAsync, _services.GetRequiredService<ILogger<NativeTlsListener>>(), 256), listeners);
            }

            var logger = _services.GetRequiredService<ILogger<NativeClientService>>();
            foreach (var (socket, _) in listeners)
                LogListening(logger, socket.LocalEndPoint);
            foreach (var (_, listener) in listeners)
                sessions.Add(RunListenerAsync(listener));
            _run = Task.WhenAll(sessions);
            foreach (var session in sessions)
            {
                if (session.IsCompleted)
                    await session.ConfigureAwait(false);
            }
        }
        catch
        {
            await _abort.CancelAsync().ConfigureAwait(false);
            foreach (var (socket, _) in listeners)
                socket.Dispose();
            await Task.WhenAll(sessions).ConfigureAwait(false);
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _graceful.CancelAsync().ConfigureAwait(false);
        try
        {
            await _run.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await _abort.CancelAsync().ConfigureAwait(false);
            await _run.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;
        try
        {
            await _abort.CancelAsync().ConfigureAwait(false);
            await _run.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _peerMaterial?.Dispose();
            _certificate?.Dispose();
            _graceful.Dispose();
            _abort.Dispose();
        }
    }

    [LoggerMessage(EventId = 3036, Level = LogLevel.Information, Message = "Listening on {Endpoint}")]
    private static partial void LogListening(ILogger logger, EndPoint? endpoint);

    private static void ValidateCertificate(X509Certificate2 certificate)
    {
        if (!certificate.HasPrivateKey)
            throw new InvalidOperationException("The TLS server certificate requires a private key.");
        foreach (var extension in certificate.Extensions)
        {
            if (extension is not X509EnhancedKeyUsageExtension usage)
                continue;
            foreach (var purpose in usage.EnhancedKeyUsages)
            {
                if (string.Equals(purpose.Value, MtlsCertificateIdentity.ServerAuthOid, StringComparison.Ordinal) || string.Equals(purpose.Value, "2.5.29.37.0", StringComparison.Ordinal))
                    return;
            }

            throw new InvalidOperationException("The TLS certificate does not permit server authentication.");
        }
    }

    private static Uri ResolveManagementUri(Uri primary)
    {
        var configured = EnvVariables.ReadString("SQUIRIX_MANAGEMENT_URI");
        var defaultPort = primary.Port <= 65533 ? primary.Port + 2 : primary.Port - 2;
        return configured == null
            ? new UriBuilder(Uri.UriSchemeHttps, IPAddress.Loopback.ToString(), defaultPort).Uri
            : ParseManagementUri(configured);
    }

    private static Uri ParseManagementUri(string configured)
    {
        var valid = Uri.TryCreate(configured, UriKind.Absolute, out var uri) && string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(uri.AbsolutePath, "/", StringComparison.Ordinal) && uri.UserInfo.Length == 0 && uri.Query.Length == 0 && uri.Fragment.Length == 0;
        return valid ? uri! : throw new InvalidOperationException("SQUIRIX_MANAGEMENT_URI requires an HTTPS origin.");
    }

    private static void AddListeners(Uri uri, Func<Socket, NativeTlsListener> create, List<(Socket Socket, NativeTlsListener Listener)> listeners)
    {
        var host = uri.DnsSafeHost;
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            Add(IPAddress.Loopback);
            if (uri.Port == 0 && listeners[^1].Socket.LocalEndPoint is IPEndPoint bound)
                uri = new UriBuilder(uri) { Port = bound.Port }.Uri;
            if (Socket.OSSupportsIPv6)
                Add(IPAddress.IPv6Loopback);
            return;
        }

        if (!IPAddress.TryParse(host, out var address))
            address = Socket.OSSupportsIPv6 ? IPAddress.IPv6Any : IPAddress.Any;
        Add(address);

        void Add(IPAddress listenAddress)
        {
            var bound = Bind(listenAddress, uri);
            try
            {
                listeners.Add((bound, create(bound)));
            }
            catch
            {
                bound.Dispose();
                throw;
            }
        }
    }

    private static Socket Bind(IPAddress address, Uri uri)
    {
        var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            if (address.Equals(IPAddress.IPv6Any))
                socket.DualMode = true;
            socket.Bind(new IPEndPoint(address, uri.Port));
            socket.Listen();
            return socket;
        }
        catch (SocketException exception)
        {
            socket.Dispose();
            throw new IOException($"Cannot bind the Squirix listener at {uri}.", exception);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private static X509Certificate2 LoadCertificate()
    {
        var path = EnvVariables.ReadString("SQUIRIX_TLS_CERTIFICATE_PATH");
        if (!string.IsNullOrWhiteSpace(path))
            return X509CertificateLoader.LoadPkcs12FromFile(path, EnvVariables.ReadString("SQUIRIX_TLS_CERTIFICATE_PASSWORD"));

        using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
        store.Open(OpenFlags.OpenExistingOnly);
        X509Certificate2? selected = null;
        var now = TimeProvider.System.GetUtcNow().UtcDateTime;
        foreach (var candidate in store.Certificates)
        {
            if (candidate.HasPrivateKey && candidate.NotBefore.ToUniversalTime() <= now && now < candidate.NotAfter.ToUniversalTime() &&
                candidate.Extensions["1.3.6.1.4.1.311.84.1.1"] != null && (selected == null || candidate.NotAfter > selected.NotAfter))
            {
                selected?.Dispose();
                selected = candidate;
            }
            else
            {
                candidate.Dispose();
            }
        }

        return selected ?? ThrowMissingCertificate();
    }

    private static X509Certificate2 ThrowMissingCertificate() => throw new InvalidOperationException("Configure SQUIRIX_TLS_CERTIFICATE_PATH or install a development HTTPS certificate.");

    private async Task RunListenerAsync(NativeTlsListener listener)
    {
        try
        {
            await listener.RunAsync(_graceful.Token, _abort.Token).ConfigureAwait(false);
        }
        catch
        {
            _services.GetRequiredService<IHostApplicationLifetime>().StopApplication();
            await _abort.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

#pragma warning disable CA2000 // Both branches transfer ownership of the returned certificate to the listener service.
    private X509Certificate2 AcquireCertificate() => _suppliedCertificate == null ? LoadCertificate() : new X509Certificate2(_suppliedCertificate);
#pragma warning restore CA2000

    private ValueTask<bool> AuthenticateManagementAsync(string bearer, CancellationToken cancellationToken)
    {
        var validator = _services.GetService<NativeConnectionAuthenticationValidator>();
        return validator == null ? ValueTask.FromResult(false) : validator.ValidateBearerAsync(bearer, cancellationToken);
    }
}
