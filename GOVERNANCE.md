# Project governance

AIKit Runtime is currently maintained by [Mehmet Ata Kartal](https://github.com/matakartal).
The maintainer is responsible for repository policy, releases, security response, and final
technical decisions.

## How changes are accepted

- Changes to `main` go through a pull request and the repository's required checks.
- Review conversations must be resolved before merge.
- Pull requests are squash-merged so `main` keeps one reviewable commit per change.
- Public behavior should preserve the canonical Rust core and its Python and Node projections.
- Security reports follow the private process in [`SECURITY.md`](SECURITY.md), not public issues.

Substantial proposals should start with an issue or discussion that explains the user outcome,
compatibility impact, and affected safety or fidelity boundary. The maintainer may reject changes
that weaken fail-closed behavior, cross-language parity, reproducibility, or documented proof
boundaries.

## Releases and project maturity

The project is in a source-first alpha phase. A release is eligible only after the protected
branch checks and the release-candidate invariants pass. Registry publishing remains disabled
until the ownership and trusted-publisher gates in [`docs/RELEASE.md`](docs/RELEASE.md) are
completed.

Governance changes use the same pull-request process as code changes. If the maintainer roster
expands, this document will be updated with nomination, removal, quorum, and release-authority
rules before those responsibilities are delegated.
