using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;

namespace Squirix.Server.Node.Hosting;

/// <summary>Option overrides for <see cref="NodeOptionsRegistration.AddSquirixValidatedOptionsAsync" />.</summary>
internal sealed class ValidatedOptionsArgs
{
    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }

    internal MtlsCertificateMaterial? MtlsMaterial { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }
}
