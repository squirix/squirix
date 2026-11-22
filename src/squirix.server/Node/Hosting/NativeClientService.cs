using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Owns the node's public TLS socket and drains it before the storage services stop.</summary>
internal sealed class NativeClientService : INodeService, IAsyncDisposable
{
    private const string Localhost = "localhost";

    private readonly IServiceProvider _services;
    private readonly TopologyOptions _topology;
    private readonly bool _mutationReplay;
    private readonly Uri? _managementUri;
    private readonly X509Certificate2? _suppliedCertificate;
    private readonly CancellationTokenSource _graceful = new();
    private readonly CancellationTokenSource _abort = new();
    private long _connectionSequence;
    private int _disposeStarted;
    private Task _run = Task.CompletedTask;
    private MtlsCertificate.Hold? _peerMaterial;
    private X509Certificate2? _certificate;
    private X509Certificate2Collection? _additionalCertificates;

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
        ListenerCertificate.Validate(_certificate);
        var tls = new SslServerAuthenticationOptions { ServerCertificateContext = SslStreamCertificateContext.Create(_certificate, _additionalCertificates, true) };
        var material = _services.Require<MtlsCertificate>();
        var peerPort = material.Enabled ? _services.Require<MtlsOptions>().InternalListenPort : 0;
        var (managementUri, managementAddresses) = await ResolveManagementBindingAsync(_certificate, peerPort, cancellationToken).ConfigureAwait(false);
        var logger = _services.Require<IServerLoggerFactory>().CreateLogger<NativeClientService>();
        _ = _services.Find<PrometheusMetricsScraper>();
        var management = new NativeManagementEndpoint(_services, AuthenticateManagementAsync, _services.Require<IServerLoggerFactory>().CreateLogger<NativeManagementEndpoint>());
        var listeners = new List<(Socket Socket, NativeTlsListener Listener)>();
        var sessions = new List<Task>();

        try
        {
            AddListeners(_topology.Uri, socket => NativeBinaryEndpoint.CreateListener(socket, tls, _services, _mutationReplay, 1024, NextConnectionId), listeners);
            var managementIndex = listeners.Count;
            AddListeners(managementUri, socket => new NativeTlsListener(socket, tls, management.ServeAsync, _services.Require<IServerLoggerFactory>().CreateLogger<NativeTlsListener>(), 64), listeners, managementAddresses);
            if (listeners[managementIndex].Socket.LocalEndPoint is IPEndPoint boundManagement)
                ManagementUri = new UriBuilder(managementUri) { Port = boundManagement.Port }.Uri;
            if (material.Enabled)
            {
                _peerMaterial = material.Retain();
                var internalUri = new UriBuilder(_topology.Uri) { Port = peerPort }.Uri;
                var feature = _services.Require<FeatureState>();
                var peer = new NativePeerEndpoint(_services, peerPort, feature.NetworkReplicationEnabled || feature.FoundationOnly);
                var verifier = new PeerHandshakeVerifier(material.PeerChains!, TimeProvider.System, _services.Require<IServerLoggerFactory>().CreateLogger<PeerHandshakeVerifier>());
                var handshake = new InboundPeerHandshake(verifier, material.NodeCertificate!, MtlsTopology.GetRemotePeerNodeIds(_topology));
                AddListeners(internalUri, socket => new NativeTlsListener(socket, handshake.CreateServerOptions, peer.ServeAsync, _services.Require<IServerLoggerFactory>().CreateLogger<NativeTlsListener>(), 256), listeners);
            }

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
            if (_additionalCertificates != null)
            {
                foreach (var certificate in _additionalCertificates)
                    certificate.Dispose();
            }

            _graceful.Dispose();
            _abort.Dispose();
        }
    }

    internal static Uri ResolveDefaultManagementUri(Uri primary, int peerPort, bool certificateMatchesLocalhost)
    {
        var port = primary.Port <= 65533 ? primary.Port + 2 : primary.Port - 2;
        if (port == peerPort)
            port = primary.Port <= 65532 ? primary.Port + 3 : primary.Port - 3;
        var host = certificateMatchesLocalhost ? Localhost : IPAddress.Loopback.ToString();
        return new UriBuilder(Uri.UriSchemeHttps, host, port).Uri;
    }

    private static void LogManagementCertificateMismatch(IServerLogger logger, Uri managementUri)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.LogManagementCertificateMismatchEvent.Write(logger, ServerLogLevel.Warning, null, ValueTuple.Create(managementUri));
    }

    private static void LogListening(IServerLogger logger, EndPoint? endpoint)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.LogListeningEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(endpoint));
    }

    private static Uri? ReadManagementUri()
    {
        var configured = EnvVariables.ReadString("SQUIRIX_MANAGEMENT_URI");
        return configured == null ? null : ParseManagementUri(configured);
    }

    private static async ValueTask<IPAddress[]?> ResolveManagementAddressesAsync(Uri uri, CancellationToken cancellationToken)
    {
        var host = uri.DnsSafeHost;
        if (string.Equals(host, Localhost, StringComparison.OrdinalIgnoreCase) || IPAddress.TryParse(host, out _))
            return null;

        var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken).ConfigureAwait(false);
        if (addresses.Length == 0)
            throw new InvalidOperationException($"The management host {host} resolved to no addresses.");
        foreach (var address in addresses)
        {
            if (address.Equals(IPAddress.Any) || address.Equals(IPAddress.IPv6Any))
                throw new InvalidOperationException($"The management host {host} resolved to a wildcard address.");
        }

        return addresses;
    }

    private static Uri ParseManagementUri(string configured)
    {
        var valid = Uri.TryCreate(configured, UriKind.Absolute, out var uri) && string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(uri.AbsolutePath, "/", StringComparison.Ordinal) && uri.UserInfo.Length == 0 && uri.Query.Length == 0 && uri.Fragment.Length == 0;
        return valid ? uri! : throw new InvalidOperationException("SQUIRIX_MANAGEMENT_URI requires an HTTPS origin.");
    }

    private static void AddListeners(
        Uri uri,
        Func<Socket, NativeTlsListener> create,
        List<(Socket Socket, NativeTlsListener Listener)> listeners,
        IPAddress[]? resolvedAddresses = null)
    {
        if (resolvedAddresses != null)
        {
            foreach (var resolved in resolvedAddresses)
                Add(resolved);
            return;
        }

        var host = uri.DnsSafeHost;
        if (string.Equals(host, Localhost, StringComparison.OrdinalIgnoreCase))
        {
            Add(IPAddress.Loopback);
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
                if (uri.Port == 0 && bound.LocalEndPoint is IPEndPoint endpoint)
                    uri = new UriBuilder(uri) { Port = endpoint.Port }.Uri;
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

    private string NextConnectionId() => Interlocked.Increment(ref _connectionSequence).ToString("x", CultureInfo.InvariantCulture);

    private async ValueTask<(Uri Uri, IPAddress[]? Addresses)> ResolveManagementBindingAsync(X509Certificate2 certificate, int peerPort, CancellationToken cancellationToken)
    {
        var configured = _managementUri ?? ReadManagementUri();
        var uri = configured ?? ResolveDefaultManagementUri(_topology.Uri, peerPort, certificate.MatchesHostname(Localhost));
        if (configured == null && !certificate.MatchesHostname(uri.IdnHost))
            LogManagementCertificateMismatch(_services.Require<IServerLoggerFactory>().CreateLogger<NativeClientService>(), uri);
        var addresses = await ResolveManagementAddressesAsync(uri, cancellationToken).ConfigureAwait(false);
        return (uri, addresses);
    }

    private async Task RunListenerAsync(NativeTlsListener listener)
    {
        try
        {
            await listener.RunAsync(_graceful.Token, _abort.Token).ConfigureAwait(false);
        }
        catch
        {
            _services.Require<INodeLifetime>().StopApplication();
            await _abort.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

#pragma warning disable CA2000 // Both branches transfer ownership of the returned certificate to the listener service.
    private X509Certificate2 AcquireCertificate()
    {
        if (_suppliedCertificate != null)
            return new X509Certificate2(_suppliedCertificate);

        var path = EnvVariables.ReadString("SQUIRIX_TLS_CERTIFICATE_PATH");
        if (string.IsNullOrWhiteSpace(path))
            return ListenerCertificate.LoadDevelopment();

        var certificates = X509CertificateLoader.LoadPkcs12CollectionFromFile(path, EnvVariables.ReadString("SQUIRIX_TLS_CERTIFICATE_PASSWORD"));
        _additionalCertificates = certificates;
        foreach (var certificate in certificates)
        {
            if (!certificate.HasPrivateKey)
                continue;

            certificates.Remove(certificate);
            return certificate;
        }

        throw new InvalidOperationException("The TLS certificate requires a private key.");
    }
#pragma warning restore CA2000

    private ValueTask<bool> AuthenticateManagementAsync(string bearer, CancellationToken cancellationToken)
    {
        var validator = _services.Find<NativeConnectionAuthenticationValidator>();
        return validator == null ? ValueTask.FromResult(false) : validator.ValidateBearerAsync(bearer, cancellationToken);
    }

    private static class ListenerCertificate
    {
        internal static void Validate(X509Certificate2 certificate)
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

        internal static X509Certificate2 LoadDevelopment()
        {
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
    }

    private static class Events
    {
        internal static readonly LogEvent LogManagementCertificateMismatchEvent = new(3037, nameof(LogManagementCertificateMismatch), "The TLS certificate does not cover the default management origin {ManagementUri}. Set SQUIRIX_MANAGEMENT_URI to a bound address covered by the certificate");

        internal static readonly LogEvent LogListeningEvent = new(3036, nameof(LogListening), "Listening on {Endpoint}");
    }
}
