# Security Policy

## Reporting a Vulnerability

Please report suspected security vulnerabilities by email:

    security@squirix.io

Do not open exported GitHub issues for security-sensitive reports.

Include as much detail as possible:

- affected package, repository, or component;
- affected version or commit, if known;
- deployment mode or configuration, if relevant;
- steps to reproduce;
- expected impact;
- proof-of-concept code or logs, if safe to share;
- any suggested mitigation.

We will review the report and respond as soon as practical.

## Supported Versions

Security fixes are prioritized for the latest released version.

Pre-release versions and unreleased development branches may change without notice.

## Security Scope

Squirix is a distributed cache library and runtime. Relevant reports may include, but are not limited to:

- authentication or authorization bypass;
- unsafe default network exposure;
- mTLS, JWT, or API-key handling issues;
- persistence, journal, manifest, or snapshot integrity issues;
- cache isolation or type-binding issues;
- denial-of-service vectors;
- package integrity or supply-chain concerns;
- sensitive data exposure through logs, metrics, traces, or diagnostics.

## Disclosure

Please give us reasonable time to investigate and prepare a fix before exported disclosure.
