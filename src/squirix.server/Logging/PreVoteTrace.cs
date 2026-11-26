using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace Squirix.Server.Logging;

/// <summary>Observes peer pre-vote phases using the trace identifier carried in existing peer metadata.</summary>
internal static class PreVoteTrace
{
    internal const string Operation = "squirix.peer.pre-vote";

    private static readonly ConditionalWeakTable<object, Action<string>> CertificateObservers = [];

    internal static event EventHandler<StageEventArgs>? Observed;

    internal static event EventHandler<TlsEventArgs>? TlsObserved;

    internal static bool HasObservers => Observed != null;

    internal static bool HasTlsObservers => TlsObserved != null;

    internal static bool IsTracing => HasObservers && string.Equals(Activity.Current?.OperationName, Operation, StringComparison.Ordinal);

    internal static void ObserveCertificates(object stream, Action<string> observer) => CertificateObservers.Add(stream, observer);

    internal static void MarkCertificate(object stream, string phase)
    {
        if (HasTlsObservers && CertificateObservers.TryGetValue(stream, out var observer))
            observer(phase);
    }

    internal static Activity? Accept(string? parent)
    {
        if (parent == null || !HasObservers)
            return null;

        var activity = new Activity(Operation);
        _ = activity.SetParentId(parent);
        _ = activity.Start();
        return activity;
    }

    internal static void MarkClientSocket(Socket socket)
    {
        if (HasObservers && Activity.Current is { } activity && string.Equals(activity.OperationName, Operation, StringComparison.Ordinal) &&
            socket.LocalEndPoint is IPEndPoint local && socket.RemoteEndPoint is IPEndPoint remote)
            Mark("send socket connected", (local.Port, remote.Port));
    }

    internal static void MarkTls((int Client, int Server) ports, string stage)
    {
        if (ports != default)
            TlsObserved?.Invoke(null, new TlsEventArgs(ports, stage));
    }

    internal static void Mark(string phase, (int Client, int Server) ports = default)
    {
        var observer = Observed;
        if (observer != null && Activity.Current is { } activity && string.Equals(activity.OperationName, Operation, StringComparison.Ordinal))
            observer(null, new StageEventArgs(activity.TraceId, phase, ports));
    }

    internal sealed class StageEventArgs : EventArgs
    {
        internal StageEventArgs(ActivityTraceId traceId, string phase, (int Client, int Server) ports)
        {
            TraceId = traceId;
            Phase = phase;
            Ports = ports;
        }

        internal ActivityTraceId TraceId { get; }

        internal string Phase { get; }

        internal (int Client, int Server) Ports { get; }
    }

    internal sealed class TlsEventArgs : EventArgs
    {
        internal TlsEventArgs((int Client, int Server) ports, string phase)
        {
            Ports = ports;
            Phase = phase;
        }

        internal (int Client, int Server) Ports { get; }

        internal string Phase { get; }
    }
}
