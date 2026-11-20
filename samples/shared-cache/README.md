# Share a cache between processes

Start a Squirix server with a trusted TLS certificate using the [server guide](../../docs/server-mode.md).
The sample connects to `https://localhost:5001` by default. Set `Squirix__Endpoint` to use another node.
If the server requires JWT authentication, set `SQUIRIX_SAMPLE_BEARER_TOKEN` for the sample processes.

From the repository root, write a typed banner in one process and read it back in another.

```shell
dotnet run --project samples/shared-cache -- put welcome "Hello from the first process"
dotnet run --project samples/shared-cache -- get welcome
```

The second process prints `Hello from the first process`. Reading an unwritten key reports a miss and exits with code 1.
