using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Internal;

/// <summary>Runs the distributed cache adapter's blocking operations over one lazily opened binary connection.</summary>
internal sealed class SynchronousDistributedCache : IDisposable
{
    private readonly Uri _endpoint;
    private readonly Lock _connect = new();
    private readonly string _cacheName;
    private readonly Func<CancellationToken, ValueTask<string>>? _tokenProvider;
    private readonly SslClientAuthenticationOptions? _tlsOptions;
    private SynchronousBinaryConnection? _connection;
    private int _disposed;

    internal SynchronousDistributedCache(Uri endpoint, Func<CancellationToken, ValueTask<string>>? tokenProvider, string cacheName, SslClientAuthenticationOptions? tlsOptions)
    {
        _endpoint = endpoint;
        _cacheName = cacheName;
        _tokenProvider = tokenProvider;
        _tlsOptions = tlsOptions;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            Volatile.Read(ref _connection)?.Dispose();
    }

    internal byte[]? Get(string key, CancellationToken token)
    {
        var request = new GetDistributedAsyncRequest { CacheName = _cacheName, Key = key };
        var response = Execute(BinaryOperation.GetDistributed, request, GetDistributedAsyncResponse.Parser, token);
        return response.Found ? response.TakeValue() : null;
    }

    internal void Set(string key, byte[] value, long? expiresAtUnixMs, long? ttlTicks, long? slidingTicks, CancellationToken token)
    {
        var request = BinaryClientProtocol.CreateSetRequest(_cacheName, key, value, expiresAtUnixMs, ttlTicks, slidingTicks);
        _ = Execute(BinaryOperation.SetDistributed, request, SetAsyncResponse.Parser, token);
    }

    internal void Refresh(string key, CancellationToken token)
    {
        var request = new RefreshDistributedAsyncRequest { CacheName = _cacheName, Key = key };
        _ = Execute(BinaryOperation.RefreshDistributed, request, RefreshDistributedAsyncResponse.Parser, token);
    }

    internal void Remove(string key, CancellationToken token)
    {
        var request = new RemoveAsyncRequest { CacheName = _cacheName, Key = key };
        _ = Execute(BinaryOperation.Remove, request, RemoveAsyncResponse.Parser, token);
    }

    private static bool IsRetryable(SquirixTransportException error) =>
        !CommitOutcomeUnknownDetail.Matches(error.Status.Detail) &&
        error.StatusCode is CacheTransportStatusCode.Unavailable or CacheTransportStatusCode.DeadlineExceeded
            or CacheTransportStatusCode.Cancelled or CacheTransportStatusCode.Internal or CacheTransportStatusCode.ResourceExhausted;

    private static SquirixTransportException TimeoutFailure(Exception? cause = null) =>
        new(new CallStatus(CacheTransportStatusCode.DeadlineExceeded, "All attempts timed out.", cause));

    private static SquirixTransportException LastFailure(SquirixTransportException? error) => error ?? TimeoutFailure();

    private static TimeSpan Backoff(long start, int attempt)
    {
        var remaining = RpcAttemptTimeout.OperationDeadline - Stopwatch.GetElapsedTime(start);
        var backoff = CallPolicyDefaults.RetryDelay(attempt);
        return remaining < backoff ? remaining : backoff;
    }

    private T Execute<T>(BinaryOperation operation, WireMessage request, WireParser<T> parser, CancellationToken token)
        where T : WireMessage
    {
        var start = Stopwatch.GetTimestamp();
        SquirixTransportException? failure = null;
        for (var attempt = 1; attempt <= RpcAttemptTimeout.MaxAttempts; attempt++)
        {
            token.ThrowIfCancellationRequested();
            var remaining = RpcAttemptTimeout.OperationDeadline - Stopwatch.GetElapsedTime(start);
            if (remaining <= TimeSpan.Zero)
                throw TimeoutFailure(failure);

            var duration = remaining < RpcAttemptTimeout.PerAttempt ? remaining : RpcAttemptTimeout.PerAttempt;
            try
            {
                return SendAttempt(operation, request, parser, duration, token);
            }
            catch (SquirixTransportException error) when (IsRetryable(error))
            {
                failure = error;
            }

            if (attempt == RpcAttemptTimeout.MaxAttempts)
                break;

            var delay = Backoff(start, attempt);
            if (delay > TimeSpan.Zero && token.WaitHandle.WaitOne(delay))
                token.ThrowIfCancellationRequested();
        }

        throw LastFailure(failure);
    }

    [SuppressMessage("Reliability", "CA2000", Justification = "GetConnection borrows the connection owned and disposed by this cache.")]
    private T SendAttempt<T>(BinaryOperation operation, WireMessage request, WireParser<T> parser, TimeSpan duration, CancellationToken token)
        where T : WireMessage
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(token);
        limit.CancelAfter(duration);
        var deadline = TimeProvider.System.GetUtcNow() + duration;
        try
        {
            return GetConnection(duration, limit.Token).Send(operation, request, parser, deadline, limit.Token, token);
        }
        catch (SquirixTransportException error) when (CommitOutcomeUnknownDetail.Matches(error.Status.Detail))
        {
            throw new CommitOutcomeUnknownException(error.Status.Detail, error);
        }
        catch (Exception error) when (error is AuthenticationException or ConnectionRejectedException)
        {
            throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unauthenticated, error.Message, error));
        }
        catch (Exception error) when (error is OperationCanceledException or IOException or SocketException)
        {
            token.ThrowIfCancellationRequested();
            throw limit.IsCancellationRequested
                ? TimeoutFailure(error)
                : new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "Binary connection closed.", error));
        }
    }

    private SynchronousBinaryConnection GetConnection(TimeSpan remaining, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _connection) is { IsAvailable: true } existing)
            return existing;

        if (!_connect.TryEnter(remaining))
            throw TimeoutFailure();
        try
        {
            token.ThrowIfCancellationRequested();
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_connection is { IsAvailable: true } current)
                return current;

            var connection = SynchronousBinaryConnection.Connect(_endpoint, _tokenProvider, _tlsOptions, token);
            Volatile.Write(ref _connection, connection);
            if (Volatile.Read(ref _disposed) == 0)
                return connection;

            connection.Dispose();
            throw new ObjectDisposedException(nameof(SynchronousDistributedCache));
        }
        finally
        {
            _connect.Exit();
        }
    }
}
