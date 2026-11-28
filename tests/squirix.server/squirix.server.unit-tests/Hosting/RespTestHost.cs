using System;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;

namespace Squirix.Server.UnitTests.Hosting;

internal sealed class RespTestHost : IAsyncDisposable
{
    private readonly X509Certificate2 _certificate;
    private readonly SquirixServer.ApplicationHandle _lifetime;

    private RespTestHost(X509Certificate2 certificate, SquirixServer.ApplicationHandle lifetime, ServerApplication server, Uri nativeUri)
    {
        _certificate = certificate;
        _lifetime = lifetime;
        Server = server;
        NativeUri = nativeUri;
    }

    internal Uri NativeUri { get; }

    internal Uri? RespUri => _lifetime.RespUri;

    internal ServerApplication Server { get; }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _lifetime.DisposeAsync();
        }
        finally
        {
            _certificate.Dispose();
        }
    }

    internal static async ValueTask<RespTestHost> StartAsync(
        CancellationToken cancellationToken,
        RespOptions? resp = null,
        TestJwtCredentials? jwt = null,
        Action<SquirixServerOptions>? configure = null,
        Action<TestServiceCollection>? register = null)
    {
        using var reserved = ListenPortPool.ServerUnitTests.HoldPort();
        var uri = new UriBuilder("https", "127.0.0.1", reserved.Port).Uri;
        using var ca = MtlsTestCertificateFactory.CreateAuthority();
        using var generated = MtlsTestCertificateFactory.CreatePeerCertificate(ca, "localhost");
        var certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
        try
        {
            var services = TestNodeBuilder.CreateServices(false);
            _ = await services.ConfigureNodeAsync(
                options =>
                {
                    options.Uri = uri;
                    options.Resp = resp ?? new RespOptions { Enabled = true, Port = 0 };
                    configure?.Invoke(options);
                },
                loadDiscoveredSettings: false,
                configureComposition: args =>
                {
                    args.ManagementUri = new Uri("https://127.0.0.1:0");
                    args.ServerCertificate = certificate;
                    args.SecurityOptions = new SecurityOptions
                    {
                        JwtSigningKey = jwt?.Base64SigningKey,
                        JwtIssuer = jwt?.Issuer,
                        JwtAudience = jwt?.Audience,
                    };
                },
                cancellationToken: cancellationToken);
            register?.Invoke(services);
            var server = services.BuildNode();
            reserved.Dispose();
            var lifetime = await SquirixServer.ApplicationHandle.StartApplicationAsync(server, static (app, token) => ServerHostingComposition.OpenStorageAsync(app.Services, token), cancellationToken);
            return new RespTestHost(certificate, lifetime, server, uri);
        }
        catch
        {
            certificate.Dispose();
            throw;
        }
    }

    internal static string Command(params string[] arguments)
    {
        var wire = new StringBuilder();
        _ = wire.Append(CultureInfo.InvariantCulture, $"*{arguments.Length}\r\n");
        foreach (var argument in arguments)
            _ = wire.Append(CultureInfo.InvariantCulture, $"${Encoding.UTF8.GetByteCount(argument)}\r\n{argument}\r\n");
        return wire.ToString();
    }

    internal static string FirstHelloReply(int protocol)
    {
        const string version = BuildMetadata.InformationalVersion;
        var header = protocol == 3 ? "%7\r\n" : "*14\r\n";
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{header}$6\r\nserver\r\n$7\r\nsquirix\r\n$7\r\nversion\r\n${version.Length}\r\n{version}\r\n$5\r\nproto\r\n:{protocol}\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n");
    }

    internal async Task<SslStream> ConnectAsync(CancellationToken cancellationToken)
    {
        var uri = RespUri!;
        var address = IPAddress.Parse(uri.Host);
#pragma warning disable CA2000 // NetworkStream takes ownership after a successful connection.
        var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
#pragma warning restore CA2000
        try
        {
            await socket.ConnectAsync(address, uri.Port, cancellationToken);
            var stream = new SslStream(new NetworkStream(socket, true));
            try
            {
                var authentication = new SslClientAuthenticationOptions
                {
                    TargetHost = "localhost",
                    RemoteCertificateValidationCallback = (_, peer, _, _) => _certificate.Equals(peer),
                };
                await stream.AuthenticateAsClientAsync(authentication, cancellationToken);
                return stream;
            }
            catch
            {
                await stream.DisposeAsync();
                throw;
            }
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}
