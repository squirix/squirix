# Cache Name Validation

The SDK and server validate logical cache names at their public boundaries. The rules below apply to new API calls.

## Rules

Cache names must satisfy all of the following:

- Required: null, empty, and whitespace-only names are rejected.
- Maximum length: 128 characters.
- Allowed ASCII characters: `A-Z`, `a-z`, `0-9`, `.`, `_`, `-`.
- Reserved names `.` and `..` are rejected.

Examples of valid names:

- `default`
- `users`
- `sessions.v1`
- `tenant-123`
- `tenant_123`

Examples of invalid names:

- empty string
- whitespace-only name (for example three spaces)
- `tenant-юзер`
- `tenant/name`
- `tenant\name`
- `.`
- `..`
- a leading space before `users`
- a trailing space after `users`

## Error Mapping

Squirix uses deterministic validation messages and does not echo raw untrusted cache-name input.

In-process APIs throw `ArgumentException` with one of these canonical messages:

- `Cache name is required.`
- `Cache name exceeds the maximum length of 128 characters.`
- `Cache name contains invalid characters. Allowed characters are A-Z, a-z, 0-9, '.', '_', and '-'.`
- `Cache name is reserved.`

Transport mapping:

- gRPC: `InvalidArgument`

Current transport note:

- gRPC requests carry the validated logical cache name; the service is not limited to the `default` namespace.
- `SquirixClient.GetCacheAsync<T>("users", cancellationToken)` selects a named cache against a remote server.
  Use `"default"` explicitly when that is the desired name; null, empty, or whitespace-only public names are rejected
  rather than silently selecting a default. See [Client API](api.md) and [server mode](server-mode.md).

## Upgrade Notes

- **0.x** preview releases may tighten validation without a deprecation period.
- When replaying the journal or loading a snapshot, recovery maps an empty namespace to `default` and preserves
  non-empty namespaces. Entries with a legacy name can therefore recover even if new API calls reject that name.
- The name rules do not change the on-disk format.
