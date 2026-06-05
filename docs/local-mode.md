# Local mode

Squirix no longer exposes an exported local, embedded, or in-process client mode.

Exported clients connect to one or more `Squirix.Server` endpoints through `SquirixClient.ConnectAsync(...)`.
`LocalCache<T>`, local mutation execution, journal, snapshots, and recovery remain internal server/runtime
implementation details.

There is no supported exported `UseLocal()`, `UseEmbedded()`, `UseInMemory()`, local `SquirixClient` factory, or
embedded exported test client. Exported API tests should start a real server host and connect remotely. Server/runtime
tests may target internal components directly only when they are explicitly testing server internals.
