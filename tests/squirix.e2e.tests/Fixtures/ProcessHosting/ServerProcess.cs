using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.E2ETests.Fixtures.ProcessHosting;

/// <summary>Runs the built server host as a child process with isolated settings and no server-library access.</summary>
internal sealed class ServerProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly DirectoryInfo _directory;
    private readonly Task<string> _output;
    private readonly Task<string> _errors;
    private readonly SocketsHttpHandler _handler;

    private ServerProcess(Process process, DirectoryInfo directory, Uri uri, string? bearerToken)
    {
        _process = process;
        _directory = directory;
        Uri = uri;
        BearerToken = bearerToken;
        _handler = new SocketsHttpHandler { UseProxy = false };
        HttpClient = new HttpClient(_handler, false) { BaseAddress = uri };
        _output = process.StandardOutput.ReadToEndAsync(CancellationToken.None);
        _errors = process.StandardError.ReadToEndAsync(CancellationToken.None);
    }

    /// <summary>Gets the node's external HTTPS endpoint.</summary>
    internal Uri Uri { get; }

    /// <summary>Gets the token accepted by the authenticated node, or null when authentication is disabled.</summary>
    internal string? BearerToken { get; }

    /// <summary>Gets an HTTP client for external health and metrics requests.</summary>
    internal HttpClient HttpClient { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!_process.HasExited)
                _process.Kill(true);

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await _process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            _ = await _output.ConfigureAwait(false);
            _ = await _errors.ConfigureAwait(false);
        }
        finally
        {
            _process.Dispose();
            HttpClient.Dispose();
            _handler.Dispose();
            _directory.Delete(true);
        }
    }

    /// <summary>Starts the built host and waits for its public readiness endpoint.</summary>
    /// <param name="authenticated">Whether JWT authentication is enabled for this node.</param>
    /// <param name="cancellationToken">Cancellation token for startup.</param>
    /// <returns>The running process, owned by the caller.</returns>
    internal static async Task<ServerProcess> StartAsync(bool authenticated, CancellationToken cancellationToken)
    {
        var hostPath = FindHostPath();
        var directory = Directory.CreateTempSubdirectory("squirix-process-smoke-");
        var uri = ReserveUri();
        var settingsPath = Path.Join(directory.FullName, "Squirix.settings.json");
        var settings = $$$$"""{"Squirix":{"Cluster":{"ClusterId":"process-smoke","NodeId":"node-a","Uri":"{{{{uri}}}}","PersistenceEnabled":false,"Peers":[{"NodeId":"node-a","Uri":"{{{{uri}}}}"}]}}}""";
        Process? process = null;
        ServerProcess? server = null;
        try
        {
            await File.WriteAllTextAsync(settingsPath, settings, cancellationToken).ConfigureAwait(false);
            var start = CreateStartInfo(hostPath, directory.FullName, settingsPath);
            var bearerToken = authenticated ? ConfigureAuthentication(start) : null;
            process = Process.Start(start) ?? ThrowStartFailed();
            server = new ServerProcess(process, directory, uri, bearerToken);
            await server.WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
            return server;
        }
        catch
        {
            if (server != null)
            {
                await server.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                process?.Dispose();
                directory.Delete(true);
            }

            throw;
        }
    }

    private static Process ThrowStartFailed() => throw new InvalidOperationException("The server process could not be started.");

    private static string ConfigureAuthentication(ProcessStartInfo start)
    {
        const string issuer = "https://process-smoke.squirix.test";
        const string audience = "process-smoke";
        var key = RandomNumberGenerator.GetBytes(32);
        start.Environment["SQUIRIX_JWT_ISSUER"] = issuer;
        start.Environment["SQUIRIX_JWT_AUDIENCE"] = audience;
        start.Environment["SQUIRIX_JWT_SIGNING_KEY"] = Convert.ToBase64String(key);
        var now = DateTime.UtcNow;
        var credentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256);
        var token = new JwtSecurityToken(issuer, audience, null, now.AddMinutes(-1), now.AddMinutes(5), credentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private static ProcessStartInfo CreateStartInfo(string hostPath, string workingDirectory, string settingsPath)
    {
        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        start.ArgumentList.Add("exec");
        start.ArgumentList.Add(hostPath);
        start.ArgumentList.Add("run");
        start.ArgumentList.Add("--settings");
        start.ArgumentList.Add(settingsPath);
        var names = new List<string>(start.Environment.Keys);
        for (var index = 0; index < names.Count; index++)
        {
            var name = names[index];
            if (name.StartsWith("SQUIRIX_", StringComparison.Ordinal))
                _ = start.Environment.Remove(name);
        }

        start.Environment["SQUIRIX_TEST_ROOT"] = workingDirectory;
        start.Environment["DOTNET_PROCESSOR_COUNT"] = "2";
        start.Environment["Logging__LogLevel__Default"] = "Warning";
        return start;
    }

    private static string FindHostPath()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null && !File.Exists(Path.Join(directory.FullName, "squirix.slnx")))
            directory = directory.Parent;

        if (directory == null)
            throw new InvalidOperationException("The repository root was not found.");

        var configuration = AppContext.BaseDirectory.Contains($"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}", StringComparison.Ordinal) ? "Release" : "Debug";
        var path = Path.Join(directory.FullName, "src", "squirix.server.host", "bin", configuration, "net10.0", "Squirix.Server.Host.dll");
        return File.Exists(path) ? path : throw new FileNotFoundException("Build the server host before running process tests.", path);
    }

    private static Uri ReserveUri()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return listener.LocalEndpoint is IPEndPoint endpoint ? new Uri($"https://localhost:{endpoint.Port}") : ThrowEndpointMissing();
    }

    private static Uri ThrowEndpointMissing() => throw new InvalidOperationException("The loopback listener did not expose an IP endpoint.");

    private async Task WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();
            if (_process.HasExited)
                throw new InvalidOperationException($"Server exited with {_process.ExitCode}: {await _output.ConfigureAwait(false)} {await _errors.ConfigureAwait(false)}");

            try
            {
                using var response = await HttpClient.GetAsync(new Uri(Uri, "/health/ready"), timeout.Token).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException ex) when (ex.HttpRequestError == HttpRequestError.ConnectionError)
            {
                // Connection refusal is expected until the child binds its listener.
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, timeout.Token).ConfigureAwait(false);
        }
    }
}
