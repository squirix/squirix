#!/usr/bin/env dotnet
#:property PublishAot=false
#:property IsAotCompatible=true
#:property ImplicitUsings=disable
#:include shared/SqrTool.cs
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using static Squirix.Tools.SqrTool;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs()[1..];
if (argv.Length == 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-package-smoke — build and run external package smoke sample.", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync(CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("Usage:", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-package-smoke.cs --", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-package-smoke.cs -- --use-existing-packages <configuration> <version>", CancellationToken.None).ConfigureAwait(false);
    return 0;
}

var reusePackages = argv is ["--use-existing-packages", _, _];
if (argv.Length > 0 && !reusePackages)
{
    await Console.Error.WriteLineAsync($"ERROR: unknown argument '{argv[0]}'", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

var repoRoot = ResolveRepoRoot();
var dotnetPath = ResolveDotnetPath();
if (dotnetPath == null)
{
    await Console.Error.WriteLineAsync("ERROR: dotnet executable path is unavailable.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

var packageDir = Path.Join(repoRoot, "artifacts", "packages");
var packageCacheRoot = Path.Join(repoRoot, "artifacts", "package-smoke-nuget");
var packageCacheDir = Path.Join(packageCacheRoot, Guid.NewGuid().ToString("N"));
_ = Directory.CreateDirectory(packageDir);
_ = Directory.CreateDirectory(packageCacheRoot);
_ = Directory.CreateDirectory(packageCacheDir);

Environment.SetEnvironmentVariable("NUGET_PACKAGES", packageCacheDir);
if (!reusePackages)
{
    foreach (var packagePath in Directory.EnumerateFiles(packageDir, "squirix.*.nupkg", SearchOption.TopDirectoryOnly))
        File.Delete(packagePath);
    foreach (var packagePath in Directory.EnumerateFiles(packageDir, "squirix.*.snupkg", SearchOption.TopDirectoryOnly))
        File.Delete(packagePath);

    foreach (var project in new[]
             {
                 "src/squirix/Squirix.csproj",
                 "src/squirix.extensions.caching/Squirix.Extensions.Caching.csproj",
                 "src/squirix.server/Squirix.Server.csproj",
             })
    {
        var exitCode = await RunDotnetAsync(
            dotnetPath, repoRoot, ["pack", project, "-c", "Release", "-o", packageDir, "-p:SquirixPackAllFrameworks=true"], CancellationToken.None).ConfigureAwait(false);
        if (exitCode != 0)
            return exitCode;
    }
}

if (!HasClientPackage(packageDir))
{
    await Console.Error.WriteLineAsync("ERROR: squirix client package was not produced.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

if (!HasServerPackage(packageDir))
{
    await Console.Error.WriteLineAsync("ERROR: squirix.server package was not produced.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

if (!HasCachingPackage(packageDir))
{
    await Console.Error.WriteLineAsync("ERROR: squirix.extensions.caching package was not produced.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

var sampleDir = Path.Join(repoRoot, "samples", "external-package-smoke");
var settingsPath = Path.Join(sampleDir, "Squirix.settings.json");
var hadSettings = File.Exists(settingsPath);
var settingsBackup = hadSettings ? await File.ReadAllBytesAsync(settingsPath, CancellationToken.None).ConfigureAwait(false) : null;

try
{
    const int maxAttempts = 5;
    var configuration = reusePackages ? argv[1] : "Release";
    foreach (var framework in new[] { "net10.0", "net11.0" })
    {
        List<string> runArguments = ["run", "-c", configuration, "-p:SmokeUsePackages=true", $"-p:SquirixTargetFramework={framework}"];
        if (reusePackages)
            runArguments.Add($"-p:SquirixPackageVersion={argv[2]}");

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var port = GetFreeTcpPort();
            var url = $"https://127.0.0.1:{port.ToString(CultureInfo.InvariantCulture)}";
            var json = BuildSettingsJson(url);
            await File.WriteAllTextAsync(settingsPath, json, CancellationToken.None).ConfigureAwait(false);

            var exitCode = await RunDotnetAsync(dotnetPath, sampleDir, runArguments, CancellationToken.None).ConfigureAwait(false);
            if (exitCode == 0)
                break;
            if (attempt == maxAttempts)
                return exitCode;
        }
    }

    return 0;
}
finally
{
    if (settingsBackup != null)
        await File.WriteAllBytesAsync(settingsPath, settingsBackup, CancellationToken.None).ConfigureAwait(false);
    else if (File.Exists(settingsPath))
        File.Delete(settingsPath);
}

static string BuildSettingsJson(string uri)
{
    var peer = new JsonObject
    {
        ["NodeId"] = "external-smoke",
        ["Uri"] = uri,
    };
    var cluster = new JsonObject
    {
        ["NodeId"] = "external-smoke",
        ["Uri"] = uri,
        ["VirtualNodes"] = 128,
        ["Peers"] = new JsonArray(peer),
    };
    var settings = new JsonObject
    {
        ["Squirix"] = new JsonObject
        {
            ["Cluster"] = cluster,
        },
    };

    return settings.ToJsonString(
        new JsonSerializerOptions
        {
            WriteIndented = true,
            RespectNullableAnnotations = false,
            RespectRequiredConstructorParameters = false,
        });
}

static int GetFreeTcpPort()
{
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    return listener.LocalEndpoint switch
    {
        IPEndPoint endpoint => endpoint.Port,
        _ => throw new InvalidOperationException("TcpListener did not expose a local IPEndPoint."),
    };
}

static bool HasClientPackage(string directory)
{
    foreach (var path in Directory.EnumerateFiles(directory, "squirix*.nupkg", SearchOption.TopDirectoryOnly))
    {
        var name = Path.GetFileName(path);
        if (name.StartsWith("squirix.", StringComparison.Ordinal)
            && !name.StartsWith("squirix.server.", StringComparison.Ordinal)
            && !name.StartsWith("squirix.extensions.", StringComparison.Ordinal))
            return true;
    }

    return false;
}

static bool HasServerPackage(string directory)
{
    using var enumerator = Directory.EnumerateFiles(directory, "squirix.server*.nupkg", SearchOption.TopDirectoryOnly).GetEnumerator();
    return enumerator.MoveNext();
}

static bool HasCachingPackage(string directory)
{
    using var enumerator = Directory.EnumerateFiles(directory, "squirix.extensions.caching.*.nupkg", SearchOption.TopDirectoryOnly).GetEnumerator();
    return enumerator.MoveNext();
}
