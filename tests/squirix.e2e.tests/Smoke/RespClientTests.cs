using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using StackExchange.Redis;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Verifies cache operations through a Redis client connected to the standalone server.</summary>
[Property("Suite", "Smoke")]
public sealed class RespClientTests
{
    /// <summary>Connects with a JWT, then exercises single-key operations over the requested protocol.</summary>
    /// <param name="protocol">The protocol the client requests during connection setup.</param>
    /// <param name="tls">Whether to enable TLS with the server flag.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(RedisProtocol.Resp2, false)]
    [Arguments(RedisProtocol.Resp3, false)]
    [Arguments(RedisProtocol.Resp2, true)]
    [Arguments(RedisProtocol.Resp3, true)]
    [Timeout(60_000)]
    public async Task RedisAndSdkShareValuesAcrossTransports(RedisProtocol protocol, bool tls, CancellationToken cancellationToken)
    {
        using var port = ProcessPortReservation.Reserve();
        await using var server = await StartRespAsync(port, cancellationToken, tls: tls);
        await using var connection = await ConnectAsync(server, port, protocol, cancellationToken, tls);
        var endpoint = connection.GetServer(IPAddress.Loopback, port.Uri.Port);
        _ = await Assert.That(endpoint.Protocol).IsEqualTo(protocol);
        var database = connection.GetDatabase(0);
        const string key = "interop:ключ";
        var binary = new byte[] { 0, 255, 13, 10, 128 };
        RedisValue value = binary;
        _ = await Assert.That((await database.StringGetAsync(key)).IsNull).IsTrue();

        var expiry = TimeSpan.FromSeconds(60);
        _ = await Assert.That(await database.StringSetAsync(key, value, expiry, When.NotExists)).IsTrue();
        _ = await Assert.That(await database.StringSetAsync(key, "other", expiry, When.NotExists)).IsFalse();
        _ = await Assert.That(await database.KeyExistsAsync(key)).IsTrue();
        _ = await Assert.That(await database.StringGetAsync(key)).IsEqualTo(value);
        await VerifySdkReadAsync(server, tls, key, binary, cancellationToken);
        var ttl = await database.KeyTimeToLiveAsync(key);
        _ = await Assert.That(ttl > TimeSpan.Zero && ttl <= expiry).IsTrue();

        _ = await Assert.That(await database.StringSetAsync(key, value, null, When.Exists)).IsTrue();
        _ = await Assert.That(await database.KeyTimeToLiveAsync(key)).IsNull();
        _ = await Assert.That(await database.KeyExpireAsync(key, expiry)).IsTrue();
        _ = await Assert.That(await database.KeyPersistAsync(key)).IsTrue();
        _ = await Assert.That(await database.KeyTimeToLiveAsync(key)).IsNull();
        _ = await Assert.That(await database.KeyDeleteAsync(key)).IsTrue();
        _ = await Assert.That(await database.KeyDeleteAsync(key)).IsFalse();
        _ = await Assert.That((await database.StringGetAsync(key)).IsNull).IsTrue();
    }

    /// <summary>Keeps key-count eviction and DEL results consistent across a crash and journal replay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task RedisDeletionOfEvictedKeySurvivesCrash(CancellationToken cancellationToken)
    {
        var directory = Directory.CreateTempSubdirectory("squirix-resp-capacity-");
        try
        {
            using (var port = ProcessPortReservation.Reserve())
            await using (var server = await StartRespAsync(port, cancellationToken, directory.FullName))
            {
                await using var client = await ConnectAsync(server, port, RedisProtocol.Resp2, cancellationToken);
                var cache = client.GetDatabase();
                _ = await Assert.That(await cache.StringSetAsync("alpha", "first")).IsTrue();
                _ = await Assert.That(await cache.StringSetAsync("beta", "second")).IsTrue();
                _ = await Assert.That(await cache.StringGetAsync("alpha")).IsEqualTo((RedisValue)"first");
                _ = await Assert.That(await cache.StringSetAsync("gamma", "third")).IsTrue();
                _ = await Assert.That((await cache.StringGetAsync("beta")).IsNull).IsTrue();
                _ = await Assert.That(await cache.KeyDeleteAsync("beta")).IsFalse();
                await server.StopAsync(cancellationToken);
            }

            using var restartedPort = ProcessPortReservation.Reserve();
            await using var restarted = await StartRespAsync(restartedPort, cancellationToken, directory.FullName);
            await using var restartedClient = await ConnectAsync(restarted, restartedPort, RedisProtocol.Resp2, cancellationToken);
            var recovered = restartedClient.GetDatabase();
            _ = await Assert.That((await recovered.StringGetAsync("beta")).IsNull).IsTrue();
            _ = await Assert.That(await recovered.StringGetAsync("alpha")).IsEqualTo((RedisValue)"first");
            _ = await Assert.That(await recovered.StringGetAsync("gamma")).IsEqualTo((RedisValue)"third");
        }
        finally
        {
            directory.Delete(true);
        }
    }

    private static Task<ServerProcess> StartRespAsync(ProcessPortReservation port, CancellationToken cancellationToken, string? dataDir = null, bool tls = true) =>
        ServerProcess.StartAsync(
            true,
            cancellationToken,
            async (start, token) =>
            {
                var path = start.ArgumentList[start.ArgumentList.IndexOf("--settings") + 1];
                var settings = JsonNode.Parse(await File.ReadAllTextAsync(path, token))!;
                var primary = new Uri(settings["Squirix"]!["Cluster"]!["Uri"]!.GetValue<string>());
                start.ArgumentList.Add("--urls");
                start.ArgumentList.Add(new UriBuilder(primary) { Scheme = "tcp" }.Uri.AbsoluteUri);
                if (tls)
                    start.ArgumentList.Add("--tls");
                settings["Squirix"]!["Resp"] = new JsonObject
                {
                    ["Port"] = -1,
                };
                await File.WriteAllTextAsync(path, settings.ToJsonString(), token);
                start.Environment["SQUIRIX_RESP_PORT"] = port.Uri.Port.ToString(CultureInfo.InvariantCulture);
                if (dataDir != null)
                {
                    start.ArgumentList.Add("--persist");
                    start.ArgumentList.Add("--data-dir");
                    start.ArgumentList.Add(dataDir);
                    start.ArgumentList.Add("--cache-eviction");
                    start.ArgumentList.Add("keyCount");
                    start.ArgumentList.Add("--max-keys");
                    start.ArgumentList.Add("2");
                }

                port.ReleaseForStart();
            });

    private static Task<ConnectionMultiplexer> ConnectAsync(ServerProcess server, ProcessPortReservation port, RedisProtocol protocol, CancellationToken cancellationToken, bool tls = true)
    {
        var options = new ConfigurationOptions
        {
            EndPoints = { { IPAddress.Loopback, port.Uri.Port } },
            AbortOnConnectFail = true,
            Protocol = protocol,
            Ssl = tls,
            SslHost = tls ? server.Uri.Host : null,
            User = "default",
            Password = server.BearerToken,
        };
        return ConnectionMultiplexer.ConnectAsync(options).WaitAsync(cancellationToken);
    }

    private static async Task VerifySdkReadAsync(ServerProcess server, bool tls, string key, byte[] expected, CancellationToken cancellationToken)
    {
        await using var client = await SquirixClient.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(tls ? server.Uri : new UriBuilder(server.Uri) { Scheme = "tcp" }.Uri);
                options.BearerTokenProvider = _ => ValueTask.FromResult(server.BearerToken!);
            },
            cancellationToken);
        var cache = await client.GetCacheAsync<byte[]>("resp", cancellationToken);
        var value = await cache.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value.AsSpan().SequenceEqual(expected)).IsTrue();
        var defaultCache = await client.GetCacheAsync<byte[]>("default", cancellationToken);
        _ = await Assert.That((await defaultCache.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }
}
