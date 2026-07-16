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

## Registry identity blocker

The `aikit` names on [PyPI](https://pypi.org/project/aikit/),
[npm](https://www.npmjs.com/package/aikit), and [crates.io](https://crates.io/crates/aikit) belong
to unrelated projects. Local artifacts in this repository therefore must not be published under
their current names without a verified ownership transfer.

Before publication, maintainers must either:

- obtain and verify ownership of every intended registry name; or
- choose a coordinated new distribution name and update Cargo package/dependency names, Python
  project/import guidance, Node.js package/type declarations, docs, examples, and release tooling.

Until then, `pip install aikit`, `npm install aikit`, and `cargo add aikit` do not install this
project and must not appear as launch instructions.

## Native distribution layout blocker

The current keyless package job proves one host-built Python wheel and one host-built npm file set.
Separate CI jobs also prove that the Node addon can be built and loaded on Linux, macOS, and
Windows. They do **not** assemble those binaries into one publishable cross-platform npm release:
`index.js` currently loads a single platform-specific `aikit_node.node`.

After the registry-name decision, choose and test a final layout (for example, final-name
platform packages selected through optional dependencies, or explicitly OS/CPU-scoped packages).
Likewise, build the final wheel matrix for every claimed Python platform. Do not relabel one
host-built native artifact as multi-platform.

## Publication order

1. Resolve the registry-name blocker and configure a verified source remote/security contact.
2. Choose one version across the workspace, Python package, Node.js package, and type stubs.
3. Package and publish the core crate first under its final name.
4. Package and publish the exact-version Rust facade after the core version is visible.
5. Implement the chosen final-name multi-platform loader/package layout, then build/test its
   platform wheels and Node.js native artifacts in clean release environments.
6. Publish PyPI/npm artifacts only after inspecting their file lists and metadata.
7. Create the signed tag/release notes from the exact commit whose evidence passed.

Registry publication, signing, and live API calls require maintainer authority and credentials;
automation must not invent success when either is absent.
