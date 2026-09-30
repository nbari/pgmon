# Security Policy

## Supported Versions

pgmon is in an early, pre-1.0 stage and changes quickly. Security fixes are
made only in the latest release; please upgrade before reporting an issue.

| Version        | Supported          |
| -------------- | ------------------ |
| Latest release | :white_check_mark: |
| Older releases | :x:                |

## Reporting a Vulnerability

Please do not open a public issue for security vulnerabilities.

Report vulnerabilities privately through
[GitHub private vulnerability reporting](https://github.com/nbari/pgmon/security/advisories/new),
or by email to [nbari@tequila.io](mailto:nbari@tequila.io). Include, when
possible:

- A description of the vulnerability and its potential impact
- Steps to reproduce the issue or a proof of concept
- Affected versions
- Any suggested mitigation or fix

You can expect an initial response within 48 hours and a status update within
seven days. The time required for a fix will depend on the vulnerability's
severity and complexity.

Please allow time for a fix to be released before publicly disclosing the
vulnerability.

## Scope

Examples of what we treat as vulnerabilities:

- Weakened TLS, such as a server certificate accepted that the configured
  `sslmode` should reject
- Credentials or other secrets leaking into logs, exported files, or the
  terminal
- pgmon running statements against the database that the user did not ask for

Limitations already documented in the [README](https://github.com/nbari/pgmon#readme)
are not treated as vulnerabilities.
