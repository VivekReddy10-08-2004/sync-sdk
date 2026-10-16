# Changelog

Notable changes to Sync SDK are recorded here. The project follows semantic
versioning while allowing breaking API changes before version 1.0.

## [Unreleased]

## [0.2.0] - 2026-10-15

### Added

- Transactional MongoDB adapter for replica sets and sharded clusters.
- Polling SQL change capture for writes made outside the gateway.
- Reproducible PostgreSQL, MySQL, MongoDB, and SQLite throughput benchmarks.
- Database concurrency benchmarks with correctness auditing in GitHub Actions.
- Benchmark results and concise comparison visuals.
- MIT licensing and a DCO-based contribution policy.

### Changed

- Documented PostgreSQL as the recommended production gateway database.
- Expanded database, packaging, observability, and deployment guidance.

### Verified

- PostgreSQL 16, MySQL 8.4, and MongoDB 8.0 completed the concurrency 1, 2, 4,
  and 8 benchmark matrix with zero failed mutations.
- The full local suite passed with 58 tests and 3 expected environment skips.

[Unreleased]: https://github.com/VivekReddy10-08-2004/sync-sdk/compare/v0.2.0...HEAD
