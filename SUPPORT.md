# Support

This page describes how to get help with EventMesh, report bugs, and engage with the community.

## Documentation

Start with these resources before opening a support request:

| Resource | Description |
|----------|-------------|
| [README.md](README.md) | Project overview and quick start |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design and component relationships |
| [ROADMAP.md](ROADMAP.md) | Milestone status and planned features |
| [docs/broker-capability-matrix.md](docs/broker-capability-matrix.md) | Broker feature comparison |
| [docs/adr/](docs/adr/) | Architecture Decision Records |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development and PR guidelines |
| [CHANGELOG.md](CHANGELOG.md) | Release notes |

## Community Channels

### GitHub Discussions

For questions, ideas, and show-and-tell:

**https://github.com/eventmesh/eventmesh/discussions**

Use Discussion categories:
- **Q&A** — How-to questions and troubleshooting
- **Ideas** — Feature proposals and design feedback
- **Show and Tell** — Share your EventMesh integrations

### Discord

Real-time community chat:

**https://discord.gg/eventmesh**

Channels:
- `#general` — Community discussion
- `#help` — Technical support from community and maintainers
- `#announcements` — Release and milestone updates
- `#contributors` — Development coordination

### GitHub Issues

For confirmed bugs and tracked feature requests:

**https://github.com/eventmesh/eventmesh/issues**

Before opening an issue:
1. Search existing issues for duplicates
2. Confirm you are on the latest version (see [CHANGELOG.md](CHANGELOG.md))
3. Include reproduction steps, .NET version, broker type, and relevant configuration

Use issue templates when available:
- **Bug Report** — Unexpected behavior with reproduction steps
- **Feature Request** — New capability proposals (reference [ROADMAP.md](ROADMAP.md) if applicable)

## Security Issues

Do **not** use GitHub Issues or Discord for security vulnerabilities.

Email **security@eventmesh.dev** following the process in [SECURITY.md](SECURITY.md).

## Commercial Support

EventMesh is an open-source project. Commercial support, SLAs, and enterprise consulting are not yet available.

Organizations requiring dedicated support may:
- Engage contributors through GitHub Sponsors (when available)
- Contact **hello@eventmesh.dev** for partnership inquiries

## Response Expectations

| Channel | Expected Response Time |
|---------|------------------------|
| GitHub Issues (bugs) | 3–5 business days for triage |
| GitHub Discussions | Best effort from community and maintainers |
| Discord `#help` | Best effort; no SLA |
| Security email | Acknowledgment within 48 hours |
| Commercial inquiries | 5 business days |

EventMesh is maintained by volunteers and contributors. Response times may vary during active milestone development.

## What to Include in a Support Request

Help us help you faster by including:

1. **EventMesh version** — NuGet package version or git commit SHA
2. **.NET version** — Output of `dotnet --version`
3. **Broker** — Which transport adapter and broker version
4. **Configuration** — Relevant `appsettings.json` or DI registration (redact secrets)
5. **Error output** — Full exception stack trace and structured log entries
6. **Reproduction** — Minimal steps or a small repro repository

## Staying Updated

- **Watch** the GitHub repository for release notifications
- **Star** the repository to show support
- Follow release notes in [CHANGELOG.md](CHANGELOG.md)
- Check [ROADMAP.md](ROADMAP.md) for milestone progress

## Code of Conduct

All support channels are governed by our [Code of Conduct](CODE_OF_CONDUCT.md). Be respectful and constructive.
