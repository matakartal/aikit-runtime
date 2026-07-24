# Source distribution guide

This candidate is distributed from its GitHub source repository. The current workflow does not
upload to npm, PyPI, or crates.io; registry ownership and publication remain explicit future
release gates. Cloning the repository is the supported installation path for this candidate.

## Use from source

```bash
git clone https://github.com/matakartal/aikit-runtime.git
cd aikit-runtime
cargo run -p aikit-runtime --example quickstart
```

Python and Node binding setup is documented in the root [README](../README.md#quick-start).

## Non-publishing checks

Run the keyless candidate gates locally:

```bash
./scripts/release-check.sh --candidate
```

Normal CI verifies Rust, Python, Node, parity, local package layouts, and supported native targets.
These checks do not contact a model provider or upload a package.

A2A evidence is a separate, keyless gate: retain the raw report from the pinned official TCK and
the exact-set verified-waiver result from `scripts/a2a-conformance.sh`. Before recording a release
candidate, verify that the general CI and required branch checks are green; a dedicated protocol
workflow passing does not cancel an unrelated CI failure.

The candidate script also requires complete Git history and fetched tags, rejects reuse of an
existing tag/evidence version for different source bytes, checks that Cargo/Python/Node versions
and exact Node platform dependencies agree, requires immutable GitHub Action SHAs and digest-pinned
manylinux images, and verifies that the checksum manifest is self-contained. Run it from a clean,
non-shallow checkout before recording evidence.

## Commit signatures

[`.github/allowed_signers`](../.github/allowed_signers) records the maintainer's SSH signing key so
any reviewer can verify a commit locally:

```bash
git -c gpg.ssh.allowedSignersFile=.github/allowed_signers verify-commit <sha>
```

History currently mixes unsigned commits with signatures that are not verifiable through this
repository's maintainer SSH allowlist, so CI does not yet gate on signatures. Once
`git config commit.gpgsign true` is consistently in effect for new maintainer commits, the step
below can verify directly pushed commits:

```yaml
# Not active: enable only after the maintainer signs commits locally.
# - name: Verify pushed commit signatures
#   if: github.event_name == 'push'
#   run: git -c gpg.ssh.allowedSignersFile=.github/allowed_signers verify-commit HEAD
```

Squash-merges performed in the GitHub web UI are signed by GitHub's own web-flow key rather than this
SSH key, so such a gate can only cover directly pushed, locally signed commits.

## Manual artifact assembly

The `release.yml` workflow is `workflow_dispatch` only. It builds local `.crate`, `.whl`, and
`.tgz` artifacts for the supported matrix, verifies that they load, writes `SHA256SUMS`, and
attests the resulting GitHub Actions artifact bundle.

The matrix covers Python ABI3 and Node native outputs for Linux x64/ARM64, macOS x64/ARM64, and
Windows x64. Each artifact is checked against the runner/native architecture or wheel platform
tag before bundling; the wrapper package is checked independently to ensure it does not embed a
host-specific addon.

Linux Python wheels and Node addons are built in digest-pinned `manylinux_2_28` containers. Their
documented compatibility floor is glibc 2.28; musl is not currently supported. After downloading
and extracting the bundle, verify it from the bundle root:

```bash
sha256sum -c SHA256SUMS
```

The workflow contains no tag trigger, registry credential, `npm publish`, PyPI upload action, or
`cargo publish` command. Its output is a temporary GitHub Actions artifact, not a public package.

## Evidence record

Artifact assembly is not complete release evidence by itself. After choosing an exact source
commit, copy [`RELEASE-EVIDENCE-TEMPLATE.md`](RELEASE-EVIDENCE-TEMPLATE.md) to
`docs/releases/vX.Y.Z.md` in a follow-up evidence commit, then record the source SHA, workflow URLs,
toolchain results, artifact hashes, and authority review without secrets. Never rewrite a
historical record to describe a newer commit.

An evidence record does not create a tag, publish a package, prove registry ownership, or certify
live-provider behavior. Those are separate explicit decisions.

## Registry publishing preparation (disabled)

[`publish.yml`](../.github/workflows/publish.yml) prepares crates.io/PyPI/npm publication behind
independent repository, environment, manifest, and registry gates. Dispatching it today fails
loudly: in addition to the locks below, the two crate names do not yet have a crates.io owner and
OIDC trusted publishing cannot create their first release.

1. the `REGISTRY_PUBLISH_ENABLED` repository variable (unset today) must equal `true`;
2. the dispatcher must type `PUBLISH` into the acknowledgement input;
3. every publishing job runs in the `registry-publish` environment, which must be created with a
   required reviewer before any job can start.

Even past all three locks, the manifests remain the final backstop: all five crates carry
`publish = false` and all six npm packages carry `"private": true`, so an accidental dispatch
cannot upload anything.

### One-time crates.io ownership bootstrap

crates.io requires each crate's first version to be published manually before a trusted publisher
can be configured. This is a separate maintainer ceremony, not a workflow setting:

1. Keep `REGISTRY_PUBLISH_ENABLED` disabled and review the exact release candidate and package
   contents. Remove `publish = false` only from the core and facade manifests as an explicit,
   reviewed release change.
2. From a secured maintainer environment, use a short-lived or immediately revocable crates.io
   token to publish `aikit-runtime-core` first. Wait until Cargo resolves that exact registry
   version, then publish `aikit-runtime`. Never place the token in the repository, GitHub Actions
   secrets, evidence files, or logs; revoke it after the bootstrap.
3. Verify both crate pages and owner access. Then configure a crates.io trusted publisher for each
   crate using repository `matakartal/aikit-runtime`, workflow `publish.yml`, and environment
   `registry-publish`.
4. Only after both owner bootstraps and trusted-publisher records exist may maintainers enable the
   guarded OIDC workflow. The workflow checks crate existence before requesting an OIDC token and
   fails with this runbook when the bootstrap is absent.

At the time of this source snapshot, both manifests still say `publish = false`; no ownership
bootstrap or real publication is claimed.

### One-time npm ownership bootstrap

npm trusted publishers are configured from an existing package's settings. Each wrapper/platform
package therefore needs a manual first release before GitHub OIDC can publish later versions:

1. Keep `REGISTRY_PUBLISH_ENABLED` disabled. Review the exact six tarballs and remove `"private":
   true` only as an explicit release change.
2. From a secured maintainer environment, use a short-lived or immediately revocable granular npm
   access token to publish the five platform packages first and `aikit-runtime` last. Every first
   publish must use the exact tag derived by `scripts/npm-release-tag.sh` (`alpha` for the current
   `0.3.0-alpha.1` candidate), never npm's implicit `latest`. Never place the token in the
   repository, GitHub Actions secrets, evidence files, or logs; revoke it after the bootstrap.
3. Verify package ownership, then configure the GitHub Actions trusted publisher separately in
   each package's npm settings for repository `matakartal/aikit-runtime`, workflow `publish.yml`,
   and environment `registry-publish`, with `publish` as the allowed action.
4. Only then enable the guarded workflow. Its npm job checks all six package names before
   downloading artifacts or attempting a trusted publish and fails with this runbook if any are
   absent.

For the manual bootstrap, derive the tag once from the reviewed source version and pass it to
every publish explicitly. `npm publish` without `--tag` is forbidden because it would assign the
current prerelease to `latest`:

```bash
VERSION="$(node -p "require('./crates/aikit-node/package.json').version")"
TAG="$(./scripts/npm-release-tag.sh "$VERSION")"
npm publish "$PLATFORM_TARBALL" --tag "$TAG"  # repeat for all five verified platform tarballs
npm publish "$WRAPPER_TARBALL" --tag "$TAG"   # publish the verified wrapper last
```

At the time of this source snapshot, all six manifests still say `"private": true`; no npm
ownership bootstrap or real publication is claimed.

Trusted-publisher OIDC covers package publication, not later `npm dist-tag` mutation. If exact
package bytes exist but their required derived tag is missing or behind, stop automated publishing.
An authenticated maintainer using a short-lived granular token must advance and verify each package:

```text
npm dist-tag add <package>@<version> <derived-tag>
npm view <package> dist-tags --json
```

The idempotent helper prints the fully resolved recovery command and never mutates a tag itself.
If the registry tag is already ahead of the requested version, the helper instead hard-fails and
explicitly forbids moving it backward; investigate the stale release rather than rolling back a tag.

Enable-time checklist (each step is a deliberate maintainer decision):

- remove `publish = false` from `crates/aikit-core/Cargo.toml` and `crates/aikit/Cargo.toml`
  only (the CLI and binding crates stay unpublishable to crates.io);
- remove `"private": true` from `crates/aikit-node/package.json` and the five
  `crates/aikit-node/npm/*/package.json` platform packages;
- complete the manual crates.io ownership bootstrap above, then configure Trusted Publishing for
  `aikit-runtime-core` and `aikit-runtime`
  (repository `matakartal/aikit-runtime`, workflow `publish.yml`, environment
  `registry-publish`), a PyPI trusted publisher for the wheel project with the same tuple, and
  complete the manual npm ownership bootstrap above, then configure npm trusted publishers for
  the wrapper and platform packages with `publish` selected as the allowed action;
- create the `registry-publish` GitHub environment with a required reviewer;
- set the `REGISTRY_PUBLISH_ENABLED` repository variable to `true`.

`release.yml` stays the artifact-assembly workflow and is unchanged: assembly is not
publication. `publish.yml` consumes an acknowledged `release.yml` run's artifacts by run id for
the wheel/npm paths and publishes crates from a fresh candidate-checked source checkout.

## Live-provider boundary

Real-provider testing remains separate and optional because it requires API keys, selected model
ids, network calls, and cost. Normal source validation stays deterministic, keyless, and
non-billable. See [LIVE-SMOKE.md](LIVE-SMOKE.md).
