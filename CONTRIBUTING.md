# Contributing

Thank you for improving Sync SDK. Bug reports, documentation fixes, tests, and
focused code changes are welcome.

## Before you start

- Search existing issues and pull requests before opening a duplicate.
- Open an issue before a large change so its behavior and scope can be agreed.
- Report security problems privately as described in [SECURITY.md](SECURITY.md).

## Make a change

Sync SDK requires Python 3.12 or newer and uses `uv` for its development
environment.

```sh
git clone https://github.com/VivekReddy10-08-2004/sync-sdk.git
cd sync-sdk
uv sync --locked --group dev --all-extras
uv run pytest -q
```

Keep changes focused. Add tests when behavior changes, update the relevant
documentation, and avoid committing credentials, local databases, generated
environments, or benchmark results from an unrepeatable setup.

Before opening a pull request, run:

```sh
uv run ruff check src tests benchmarks
MYPYPATH=src uv run mypy src benchmarks/sync_throughput.py
uv run pytest -q
```

Database-specific changes should also pass the relevant live adapter contract.
The pull request workflow runs PostgreSQL, MySQL, and MongoDB services.

## Commit certification and license

This project uses the [Developer Certificate of Origin 1.1](https://developercertificate.org/)
instead of a separate Contributor License Agreement. Add a `Signed-off-by` line
to every commit with:

```sh
git commit --signoff
```

The sign-off certifies that you wrote the contribution, have the right to submit
it, and understand that it is public. Use your real name and an email address you
control. If needed, add a sign-off to your latest commit with
`git commit --amend --signoff`.

Contributions are accepted under the repository's [MIT License](LICENSE), using
the same license for incoming contributions and outgoing releases. You retain
copyright in your contribution. This policy does not transfer your copyright or
give the project permission to relicense your work under incompatible terms.

## Pull requests

Explain the problem and resulting behavior, link related issues, and include the
commands used for validation. Maintainers may ask for changes to keep the public
API, storage guarantees, and documentation consistent.
