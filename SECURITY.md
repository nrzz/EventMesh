# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

Security fixes are backported to the latest minor release in the supported major version.

## Reporting a Vulnerability

We take the security of EventMesh seriously. If you discover a security vulnerability, please report it responsibly.

**Do not report security vulnerabilities through public GitHub issues.**

Instead, email **security@eventmesh.dev** with:

- A description of the vulnerability
- Steps to reproduce the issue
- Affected components (e.g., `EventMesh.Core`, `EventMesh.Transport.Kafka`, Management API)
- Potential impact assessment (confidentiality, integrity, availability)
- Any suggested fixes (optional)

You should receive an acknowledgment within 48 hours. We will work with you to understand and address the issue promptly.

## Disclosure Policy

1. We confirm receipt of your report within 48 hours
2. We provide regular updates on investigation and remediation progress
3. Once a fix is available, we coordinate disclosure timing with you
4. Critical vulnerabilities (remote code execution, authentication bypass, message tampering) are targeted for resolution within 7 days
5. Non-critical vulnerabilities are addressed in the next scheduled release

We follow coordinated disclosure and credit reporters in the security advisory (unless you prefer to remain anonymous).

## Security Scope

### In scope

- EventMesh core libraries (`EventMesh.Abstractions`, `EventMesh.Core`)
- Transport adapter packages (`EventMesh.Transport.*`)
- Storage packages (`EventMesh.Storage.*`)
- Management API and dashboard (`EventMesh.Management.Api`, dashboard)
- CLI tool (`EventMesh.Cli`)
- Plugin SDK and first-party plugins
- CI/CD pipeline configuration in this repository
- Docker Compose and deployment manifests shipped with EventMesh

### Out of scope

- Vulnerabilities in underlying message brokers (report to the respective vendor)
- Vulnerabilities in third-party dependencies already fixed in a newer version we have not yet adopted (report via GitHub Dependabot or email us)
- Denial of service via intentionally misconfigured resource limits in user deployments
- Social engineering attacks

## Security Architecture

EventMesh implements defense in depth across the data plane and control plane:

| Layer | Controls |
|-------|----------|
| Transport | TLS for all broker connections; credential providers for Vault, AWS Secrets Manager, Azure Key Vault |
| Message payload | Optional AES-GCM encryption via plugins; CloudEvents integrity attributes |
| Storage | PostgreSQL connection encryption; parameterized queries (Dapper) to prevent SQL injection |
| Management API | OAuth2/OIDC, JWT, API key authentication; RBAC for administrative operations (Milestone 15) |
| Observability | No sensitive payload data in logs or traces by default; correlation IDs only |

## Security Best Practices for Deployers

When deploying EventMesh in production:

- **Enable TLS** — Configure TLS for all broker connections and management API endpoints
- **Rotate credentials** — Use short-lived credentials via secret managers; never commit secrets to source control
- **Restrict network access** — Do not expose PostgreSQL, Redis, or broker ports to the public internet
- **Principle of least privilege** — Grant transport adapters only the broker permissions they require
- **Keep dependencies updated** — Monitor GitHub security advisories and apply patch releases promptly
- **Disable control plane in production** if not needed — The data plane operates independently; management API exposure should be network-restricted
- **Review plugin sources** — Only load plugins from trusted NuGet feeds or signed assemblies
- **Enable payload encryption** — For sensitive data, enable the AES-GCM encryption plugin with keys managed by a secrets provider

## Threat Model

A formal threat model document will be published as part of Milestone 15 (Production Hardening). It will cover:

- Message interception and tampering
- Unauthorized publish/consume access
- Replay attacks and idempotency bypass
- Plugin supply chain risks
- Management API privilege escalation
- Multi-tenant isolation in shared broker environments

## Security Updates

Security advisories are published as [GitHub Security Advisories](https://github.com/nrzz/EventMesh/security/advisories) and noted in [CHANGELOG.md](CHANGELOG.md).

Subscribe to repository notifications or watch the `security` label to receive updates.

## Acknowledgments

We thank security researchers who responsibly disclose vulnerabilities. Contributors will be acknowledged in the advisory unless they request anonymity.
