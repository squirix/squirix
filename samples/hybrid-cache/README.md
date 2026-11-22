# HybridCache with Squirix as L2

Start a Squirix server with a trusted TLS certificate using the [server guide](../../docs/server-mode.md).
The sample connects to `https://localhost:5001` by default. Set `Squirix__Endpoint` to use another node.
If the server requires JWT authentication, set `SQUIRIX_SAMPLE_BEARER_TOKEN` for the sample processes.

From the repository root, write a value, then read the same key from a new process.

```shell
dotnet run --project samples/hybrid-cache -- set demo
dotnet run --project samples/hybrid-cache -- get demo
```

The first process creates a value containing its process ID. The second prints the same ID because it retrieves the value
from Squirix rather than calling the factory.

See the [provider guide](../../docs/distributed-cache.md) for installation, expiration and L1 behavior.
