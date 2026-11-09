# HybridCache with Squirix as L2

Start a Squirix server on `https://localhost:51907` with a trusted development HTTPS certificate. Then run this sample twice in separate terminals:

```shell
dotnet run --project samples/aspnet-hybrid-cache -- --urls http://localhost:51908
dotnet run --project samples/aspnet-hybrid-cache -- --urls http://localhost:51909
```

Request `/messages/demo` from port 51908, then from port 51909. Both responses contain the process ID of the first application. The second application reads the cached response from Squirix.

To use another server, pass `--CacheEndpoint https://your-server:7443`. See [the provider guide](../../docs/distributed-cache.md) for installation, expiration, and L1 behavior.
