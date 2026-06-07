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

### Submit changes

1. Fork the repo and create a branch:

   ```bash
   git checkout -b my-fix
   ```

2. Make your change and run tests:

   ```powershell
   dotnet build squirix.slnx --configuration Release
   dotnet test squirix.slnx --configuration Release
   ```

3. Open a pull request with a short description.

## Guidelines

- Keep code simple and readable.
- Add tests when you change behavior.
- Be respectful in discussions.

## Documentation

Documentation-only pull requests must pass `npx markdownlint-cli2` (see `.markdownlint-cli2.jsonc` in the repository root).

## License

By contributing, you agree your code is under the [Apache 2.0 License](LICENSE).
