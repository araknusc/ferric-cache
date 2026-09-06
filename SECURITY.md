# Security Policy

## Supported versions

ferric-cache is pre-1.0. Only the latest release on `main` receives security
fixes.

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Use GitHub's private vulnerability reporting for this repository
(**Security → Report a vulnerability**). You should get an acknowledgement
within a few days. Once a fix is ready it will be released and the report
credited to you unless you prefer otherwise.

## Scope notes

- The default configuration binds to `127.0.0.1` with authentication and TLS
  disabled. Anyone exposing the server on a network is expected to enable the
  `security` and `tls` sections of the config.
- `config.secure.example.json` contains placeholder passwords. They are
  examples only and must be replaced before use.
- Lua `EVAL` runs untrusted scripts under the caller's ACL. Sandbox escapes or
  ACL bypasses via scripting are in scope.
