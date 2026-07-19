using System;
using System.Collections.Generic;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server;

/// <summary>Validates <see cref="SquirixServerOptions" /> using cluster topology rules.</summary>
internal static class SquirixServerOptionsValidator
{
    internal static void Validate(SquirixServerOptions options)
    {
        if (!TryValidate(options, out var errors))
            throw new ArgumentException(errors[0], nameof(options));
    }

    internal static bool TryValidate(SquirixServerOptions options, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var peers = new ServerPeer[options.Peers.Count is 0 ? 1 : options.Peers.Count];
        if (options.Peers.Count is 0)
        {
            peers[0] = new ServerPeer { NodeId = options.NodeId, Uri = options.Uri };
        }
        else
        {
            for (var i = 0; i < options.Peers.Count; i++)
            {
                var peer = options.Peers[i];
                peers[i] = new ServerPeer { NodeId = peer.NodeId, Uri = peer.Uri };
            }
        }

        var topology = new TopologyOptions(peers)
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            Uri = options.Uri,
            VirtualNodes = options.VirtualNodes,
        };

        return TopologyValidator.TryValidate(topology, options.PersistenceEnabled, options.DataDirectory, out errors);
    }
}
