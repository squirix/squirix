using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

/// <summary>Option overrides for <see cref="NodeOptionsRegistration.AddSquirixValidatedOptionsAsync" />.</summary>
internal sealed class ValidatedOptionsArgs
{
    internal TriggerOptions? SnapshotOptions { get; init; }

    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    internal MtlsCertificateMaterial? MtlsMaterial { get; init; }
}
