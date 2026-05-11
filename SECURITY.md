# Security Policy

## Supported versions

Security fixes are prioritized for the current `1.x` release line.

| Version | Supported |
| --- | --- |
| `1.x` | Yes |
| `< 1.0` | No |

## Reporting a vulnerability

Please do not open a public GitHub issue for suspected security vulnerabilities.

Email the maintainer at `ichingasamuel@gmail.com` with:

- A short description of the issue.
- A minimal reproduction or proof of concept, if available.
- Affected versions or commits.
- Your environment details.
- Any known mitigations.

You should receive an acknowledgement within 7 days. If the report is valid, the maintainer will coordinate a fix and public disclosure timing.

## Scope

Useful reports include vulnerabilities in:

- Task execution, cancellation, or timeout behavior that can cause unexpected code execution.
- Process handling that can leak resources or bypass intended cancellation.
- Packaging or release artifacts.
- Documentation examples that create unsafe defaults.

Out of scope:

- Vulnerabilities requiring malicious code to be intentionally submitted as a task by the application owner.
- Denial-of-service behavior caused solely by untrusted task functions consuming CPU, memory, disk, or network resources.
- Reports against unsupported Python versions.

## Security expectations for users

`osiiso` executes callables supplied by your application. Treat task functions and their arguments as trusted application code. Do not submit untrusted user-provided Python callables to any queue backend.
