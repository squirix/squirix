using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Why a peer certificate was refused by the chain check.</summary>
/// <param name="ChainStatus">The chain status flags of the failed build; <see cref="X509ChainStatusFlags.NoError" /> when the chain could not be built at all or did not fail.</param>
/// <param name="MissingNodeId">Whether the chain was trusted but the certificate names no NodeId.</param>
[StructLayout(LayoutKind.Auto)]
internal readonly record struct PeerChainRefusal(X509ChainStatusFlags ChainStatus, bool MissingNodeId);
