using System;
using System.Runtime.CompilerServices;
using Squirix.Transport.Binary;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>A wire-level refusal exposed to black-box tests.</summary>
public sealed class WireCallException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="WireCallException" /> class.</summary>
    public WireCallException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WireCallException" /> class.</summary>
    /// <param name="message">The detail.</param>
    public WireCallException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WireCallException" /> class.</summary>
    /// <param name="message">The detail.</param>
    /// <param name="innerException">The cause.</param>
    public WireCallException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    internal WireCallException(ServerCallException error)
        : base(error.Message, error)
    {
        StatusCode = Unsafe.BitCast<FrameStatus, WireStatus>(error.StatusCode);
    }

    internal WireCallException(WireStatus code, string detail)
        : base(detail)
    {
        StatusCode = code;
    }

    internal WireStatus StatusCode { get; } = WireStatus.Unknown;
}
