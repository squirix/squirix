using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Squirix.Testing;

namespace Squirix.E2ETests.Fixtures.ProcessHosting;

/// <summary>Runs the built server host as a child process with isolated settings and no server-library access.</summary>
internal sealed class ServerProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly DirectoryInfo _directory;
    private readonly Task<string> _output;
    private readonly Task<string> _errors;
    private readonly SocketsHttpHandler _handler;
    private readonly ProcessPortReservation _port;
    private readonly ProcessPortReservation _management;

    private ServerProcess(Process process, DirectoryInfo directory, ProcessPortReservation port, ProcessPortReservation management, string? bearerToken)
    {
        _process = process;
        _directory = directory;
        _port = port;
        _management = management;
        Uri = port.Uri;
        BearerToken = bearerToken;
        _handler = new SocketsHttpHandler { UseProxy = false };
        HttpClient = new HttpClient(_handler, false) { BaseAddress = management.Uri };
        _output = process.StandardOutput.ReadToEndAsync(CancellationToken.None);
        _errors = process.StandardError.ReadToEndAsync(CancellationToken.None);
    }

    /// <summary>Gets the node's external HTTPS endpoint.</summary>
    internal Uri Uri { get; }

    internal Uri ManagementUri => _management.Uri;

    /// <summary>Gets the token accepted by the authenticated node, or null when authentication is disabled.</summary>
    internal string? BearerToken { get; }

    /// <summary>Gets an HTTP client for external health and metrics requests.</summary>
    internal HttpClient HttpClient { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                await StopAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested)
            {
                // The child was killed before the wait; retain its reservation until termination is confirmed.
                await _process.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);
            }

            _port.Dispose();
            _management.Dispose();
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
    /// <param name="configure">Optional child-process configuration applied before startup.</param>
    /// <returns>The running process, owned by the caller.</returns>
    internal static async Task<ServerProcess> StartAsync(bool authenticated, CancellationToken cancellationToken, Func<ProcessStartInfo, CancellationToken, Task>? configure = null)
    {
        var hostPath = FindHostPath();
        var port = ProcessPortReservation.Reserve();
        DirectoryInfo directory;
        try
        {
            directory = Directory.CreateTempSubdirectory("squirix-process-smoke-");
        }
        catch
        {
            port.Dispose();
            throw;
        }

        var uri = port.Uri;
        var settingsPath = Path.Join(directory.FullName, "appsettings.json");
        var settings = $$$$"""{"Squirix":{"Cluster":{"ClusterId":"process-smoke","NodeId":"node-a","Uri":"{{{{uri}}}}","PersistenceEnabled":false,"Peers":[{"NodeId":"node-a","Uri":"{{{{uri}}}}"}]}}}""";
        ProcessPortReservation? management = null;
        Process? process = null;
        ServerProcess? server = null;
        try
        {
            await File.WriteAllTextAsync(settingsPath, settings, cancellationToken).ConfigureAwait(false);
            management = ProcessPortReservation.Reserve();
            var start = CreateStartInfo(hostPath, directory.FullName, settingsPath);
            start.Environment["SQUIRIX_MANAGEMENT_URI"] = management.Uri.AbsoluteUri;
            if (configure != null)
                await configure(start, cancellationToken).ConfigureAwait(false);
            var bearerToken = authenticated ? ConfigureAuthentication(start) : null;
            port.ReleaseForStart();
            management.ReleaseForStart();
            process = Process.Start(start) ?? ThrowStartFailed();
            server = new ServerProcess(process, directory, port, management, bearerToken);
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
                management?.Dispose();
                process?.Dispose();
                port.Dispose();
                directory.Delete(true);
            }

            throw;
        }
    }

    /// <summary>Gets all captured standard output after the process has stopped.</summary>
    /// <returns>The captured output.</returns>
    internal Task<string> ReadOutputAsync() => _output;

    /// <summary>Terminates the child process and waits for the port to close.</summary>
    /// <param name="cancellationToken">Cancellation for the exit wait.</param>
    /// <returns>A task that completes after process exit.</returns>
    internal Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_process.HasExited)
            _process.Kill(true);

        return _process.WaitForExitAsync(cancellationToken);
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
        var path = Path.Join(directory.FullName, "src", "squirix.server.host", "bin", configuration, PathKit.GetTargetFrameworkDirectory(), "Squirix.Server.Host.dll");
        return File.Exists(path) ? path : throw new FileNotFoundException("Build the server host before running process tests.", path);
    }

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
                using var response = await HttpClient.GetAsync(new Uri(ManagementUri, "/health/ready"), timeout.Token).ConfigureAwait(false);
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
