using System;
using System.Net;
using Squirix.Server.Attributes;

namespace Squirix.Server;

/// <summary>Configures the RESP endpoint.</summary>
[Immutable]
public sealed class RespOptions
{
    internal const string DefaultBindAddress = "127.0.0.1";
    internal const int DefaultPort = 6379;

    /// <summary>Initializes a new instance of the <see cref="RespOptions" /> class with the listener enabled on the IPv4 loopback address.</summary>
    public RespOptions()
    {
    }

    /// <summary>Gets a value indicating whether the RESP listener starts. The default is enabled.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Gets the IP address to bind. The default is IPv4 loopback.</summary>
    public string BindAddress { get; init; } = DefaultBindAddress;

    /// <summary>Gets the TCP port to bind. The default is 6379. Use zero to allocate a port.</summary>
    public int Port { get; init; } = DefaultPort;

    internal Uri? ToListenUri(Uri primary)
    {
        if (!Enabled)
            return null;
        var error = Validate();
        var scheme = string.Equals(primary.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal) ? "rediss" : "redis";
        return error == null ? new UriBuilder(scheme, BindAddress, Port).Uri : throw new InvalidOperationException(error);
    }

    internal string? Validate()
    {
        if (!Enabled)
            return null;
        if (Port is < IPEndPoint.MinPort or > IPEndPoint.MaxPort)
            return "RESP port must be between 0 and 65535.";

        var validAddress = IPAddress.TryParse(BindAddress, out _);
        return validAddress ? null : "RESP requires an IP address for its bind address.";
    }
}
