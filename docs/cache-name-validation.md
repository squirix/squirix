# Cache Name Validation

Squirix validates logical cache names through one centralized rule set before runtime resolution, mutation execution,
journal writes, diagnostics, or routing can observe the name.

## Rules

Cache names must satisfy all of the following:

- Required: null, empty, and whitespace-only names are rejected.
- Maximum length: 128 characters.
- Allowed characters only: `A-Z`, `a-z`, `0-9`, `.`, `_`, `-`.
- ASCII only: Unicode and other non-ASCII characters are rejected.
- No control characters.
- No path separators: `/` and `\` are rejected.
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

- gRPC cache routes operate on the validated default cache namespace.
- Named caches are accessed through `SquirixClient.GetCacheAsync` against a remote `Squirix.Server` endpoint (see
  [server-mode.md](server-mode.md)).

## Upgrade Notes

- **0.x** preview releases may tighten validation without a deprecation period.
- Persisted data that already contains invalid cache namespaces may require cleanup or migration before upgrade.
- The persistence format itself is unchanged by this validation tightening.
