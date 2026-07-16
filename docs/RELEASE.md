# Release checklist

The repository is an implementation candidate until a maintainer completes and records these
steps. Passing keyless CI alone is not a live-provider or registry release.

Run the non-billable candidate invariants during development:

```bash
./scripts/release-check.sh --candidate
```

For a real version, copy [`RELEASE-EVIDENCE-TEMPLATE.md`](RELEASE-EVIDENCE-TEMPLATE.md) to
`releases/vX.Y.Z.md`, complete it without secrets, and run `./scripts/release-check.sh --release`.
Release mode intentionally fails on a placeholder version, missing commit/remote metadata, a dirty
tree, unresolved public contacts, or incomplete authority/live/native-artifact evidence.

## Required evidence

- A committed SHA exists on the verified source remote and the GitHub Actions workflow passes for
  that exact SHA; local commands in an uncommitted checkout are not CI evidence.
- Strict Rust formatting, Clippy, tests, rustdoc, and the declared MSRV pass from a clean checkout.
- Python and Node.js native packages build, their keyless scenarios pass, and Rust/Python/Node.js
  parity matches the canonical contract byte for byte.
- Cargo, wheel, and npm package contents are inspected; license/readme/type metadata and native
  artifacts are present for each target platform.
- The opt-in full live matrix passes text, structured output, governed denial, and two-request
  replay for all four providers. Record the date, commit SHA, and model ids without recording keys
  or private response data.
- The containment capability report is reviewed on each supported release platform. An unavailable
  required backend must deny execution rather than silently run uncontained.
- The threat model, security policy, changelog/release notes, and known limitations match the code.
- A real source remote and private security-reporting contact replace all TBD hosting metadata.
- The versioned evidence record points to the exact committed SHA and contains no credentials,
  private prompts, or raw provider responses.

## Registry identity

The bare `aikit` names on PyPI, npm, and crates.io belong to unrelated projects. The coordinated
distribution identity is therefore:

- Rust facade: `aikit-runtime` (library import remains `aikit`)
- Rust core: `aikit-runtime-core`
- Python distribution: `aikit-runtime` (Python import remains `aikit`)
- npm wrapper: `aikit-runtime`
- npm native packages: `aikit-runtime-{darwin-arm64,darwin-x64,linux-arm64-gnu,linux-x64-gnu,win32-x64-msvc}`

The names were checked as available before implementation, but availability is not ownership.
The release record must still prove that the maintainer authenticated to each registry and
reserved/published the intended names. Never publish this repository under bare `aikit`.

## Native distribution layout

The npm wrapper uses exact-version optional native packages selected by platform/architecture.
The release workflow builds and smoke-loads each package on its matching hosted runner, then
assembles the wrapper only after all target artifacts exist. Python uses ABI3 wheels built per
claimed platform. Musl Linux and Windows ARM64 are not claimed in v0.1.0.

## Publication order

1. Verify registry authority and the source remote/security contact.
2. Choose one version across the workspace, Python package, Node.js package, and type stubs.
3. Package and publish the core crate first under its final name.
4. Package and publish the exact-version Rust facade after the core version is visible.
5. Build/test the final-name platform wheels and Node.js native packages in clean release environments.
6. Publish PyPI/npm artifacts only after inspecting their file lists and metadata.
7. Create the signed tag/release notes from the exact commit whose evidence passed.

Registry publication, signing, and live API calls require maintainer authority and credentials;
automation must not invent success when either is absent.
