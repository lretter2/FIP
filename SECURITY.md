# Security Policy

## Supported Versions

The Financial Intelligence Platform (FIP) is a continuously deployed cloud-native platform, not a versioned software release. Security patches are applied to the production environment on an ongoing basis.

| Environment | Security Updates   |
| ----------- | ------------------ |
| `prod`      | :white_check_mark: |
| `dev`       | :white_check_mark: |
| `ci`        | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in FIP, please report it **privately** to the Platform Engineering team. Do **not** open a public GitHub issue for security vulnerabilities.

**How to report:**

1. Post a **private** message in the internal TEAMS channel `#fip-platform-security`, or send an email to the address pinned in that channel.
2. Include a description of the vulnerability, steps to reproduce, and potential impact.
3. You will receive an acknowledgement within **48 hours**.
4. A full assessment and remediation plan will be communicated within **14 days**.

If the vulnerability is accepted, a fix will be prioritised based on severity (P1 critical issues are patched within 24 hours; P2 issues within 7 days). If the report is declined, you will receive an explanation.

For detailed security architecture, controls, and compliance information see [`doc/SECURITY_AND_COMPLIANCE.md`](doc/SECURITY_AND_COMPLIANCE.md).
