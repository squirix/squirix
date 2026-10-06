using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>One hold on shared <see cref="MtlsCertificate" /> material; disposing it releases the hold exactly once.</summary>
[Mutable]
internal sealed class MtlsCertificateHold : IDisposable
{
    private readonly MtlsCertificate _material;

    private int _released;

    internal MtlsCertificateHold(MtlsCertificate material)
    {
        ArgumentNullException.ThrowIfNull(material);
        _material = material;
    }

    /// <summary>Gets a value indicating whether this hold was already released.</summary>
    internal bool IsReleased => Volatile.Read(ref _released) == 1;

    /// <summary>Releases the hold; the certificates are freed when it was the last one. Repeated calls do nothing.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _released, 1) == 1)
            return;

        _material.Release();
    }
}
