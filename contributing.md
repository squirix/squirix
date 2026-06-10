# Contributing to squirix

Thanks for checking out squirix. **squirix 0.1.0** is an early preview — we especially welcome bug reports, API
feedback, and durability questions via [GitHub Issues](https://github.com/squirix/squirix/issues).

## How to help

### Report bugs

- Open an issue at [github.com/squirix/squirix/issues](https://github.com/squirix/squirix/issues) with clear steps to reproduce.
- Add logs, error messages, or screenshots when possible.

### Suggest features

- Use issues for proposals or ideas.
- Keep it short and explain the use case.

### Branching model

- **`develop`** — integration branch for day-to-day work. Open pull requests against `develop`.
- **`main`** — stable release branch. Changes land here via a release pull request (`develop` → `main`).
- **Releases** — tag a commit on `main` with `v*` (for example `v0.2.0`) to trigger the Release workflow.

Typical flow:

```text
feature/my-fix → develop → main → tag vX.Y.Z
```

### Submit changes

1. Fork the repo, branch from `develop`, and create a feature branch:

   ```bash
   git checkout develop
   git pull
   git checkout -b my-fix
   ```

2. Make your change and run tests:

   ```powershell
   dotnet build squirix.slnx --configuration Release
   dotnet test squirix.slnx --configuration Release
   ```

3. Open a pull request targeting `develop` with a short description.
   Link related issues with closing keywords (`Fixes #123`, `Closes #123`, or `Resolves #123`) in the PR
   title or body. GitHub only auto-closes issues on merge to the default branch (`main`); this repository
   closes linked issues when the PR merges into `develop` via
   [`.github/workflows/close-linked-issues-on-develop.yml`](./.github/workflows/close-linked-issues-on-develop.yml).

## Guidelines

- Keep code simple and readable.
- Add tests when you change behavior.
- Be respectful in discussions.

## Documentation

Documentation-only pull requests must pass `npx markdownlint-cli2` (see `.markdownlint-cli2.jsonc` in the repository
root). CI runs the same check automatically when a pull request changes Markdown files under `docs/`, `README.md`, or
other `*.md` paths matched by the workflow filter.

## License

By contributing, you agree your code is under the [Apache 2.0 License](./LICENSE).
