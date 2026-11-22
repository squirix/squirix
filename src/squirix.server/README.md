# squirix.server

The `squirix.server` NuGet package exposes the Squirix server runtime. Install `squirix.server.tool` for the
standalone `squirix-server` CLI and `squirix` for client applications.

| Type | Role |
| --- | --- |
| `SquirixServer` | Starts a node and owns its asynchronous disposal |
| `Configurator` | Loads and validates node settings |
| `SquirixServerOptions` and `SquirixServerPeerOptions` | Configure listen origin, cluster topology and persistence |

Start a node with explicit options:

```csharp
using System;
using Squirix.Server;

var options = new SquirixServerOptions
{
    NodeId = "node-a",
    Uri = new Uri("https://localhost:5001"),
};
await using var server = await SquirixServer.StartAsync(options);
```

The standalone tool and embedded node share the same runtime. Clients connect over binary TLS, and health and
metrics are available through the management listener. See [server mode](../../docs/server-mode.md) for a
complete client/server example and [configuration](../../docs/configuration.md) for certificates and ports.

The server and SDK compile the shared binary cache codec. The server runtime does not reference the client
SDK assembly.
