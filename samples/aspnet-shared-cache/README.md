# Share a cache between ASP.NET Core instances

This local example runs one Squirix server and two instances of the same ASP.NET Core application.
The first instance writes a typed banner to the server; the second reads it.
The application uses the Squirix SDK directly, not `IDistributedCache`.

## Requirements

- .NET 10 SDK
- A trusted .NET development HTTPS certificate

## 1. Start the cache server

In a terminal, install the preview CLI and start an in-memory server:

```bash
dotnet dev-certs https --trust
dotnet tool install --global squirix.server.tool --version 0.1.0-preview.10
squirix-server run
```

Leave the server running at `https://localhost:5001`. Its default local configuration allows any local process to access the cache.
Use it only for local development.
If port 5001 is in use, choose a free HTTPS port with `squirix-server run --urls https://localhost:5443`
and set `Squirix__Endpoint=https://localhost:5443` for both app instances.

## 2. Start two application instances

From this directory, build once:

```bash
dotnet build
```

Open two more terminals in this directory and run one command in each:

```bash
dotnet run --no-build --no-launch-profile --urls http://localhost:5101
```

```bash
dotnet run --no-build --no-launch-profile --urls http://localhost:5102
```

The app endpoints use HTTP on loopback for this local example. Each app connects to Squirix over HTTPS with certificate verification enabled.

## 3. Write through the first instance and read through the second

In another terminal, use `curl` (the commands below use Bash quoting):

```bash
curl -i http://localhost:5102/banner/not-written
```

The response is `HTTP/1.1 404 Not Found` because the key has not been written.

```bash
curl -i -X PUT http://localhost:5101/banner/welcome -H "Content-Type: application/json" -d '{"message":"Hello from the first instance"}'
curl -i http://localhost:5102/banner/welcome
```

The first response is `204 No Content`. The second is `200 OK` with:

```json
{"message":"Hello from the first instance"}
```

Stop both app instances and the server with Ctrl+C in their terminals. The default server is in-memory, so its values are lost when it stops.
