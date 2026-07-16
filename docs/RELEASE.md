# Source distribution guide

`aikit-runtime` is distributed from its GitHub source repository. npm, PyPI, and crates.io uploads
are intentionally out of scope. Cloning the repository is the official installation path.

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

## Manual artifact assembly

The `release.yml` workflow is `workflow_dispatch` only. It builds local `.crate`, `.whl`, and
`.tgz` artifacts for the supported matrix, verifies that they load, writes `SHA256SUMS`, and
attests the resulting GitHub Actions artifact bundle.

The workflow contains no tag trigger, registry credential, `npm publish`, PyPI upload action, or
`cargo publish` command. Its output is a temporary GitHub Actions artifact, not a public package.

## Live-provider boundary

Real-provider testing remains separate and optional because it requires API keys, selected model
ids, network calls, and cost. Normal source validation stays deterministic, keyless, and
non-billable. See [LIVE-SMOKE.md](LIVE-SMOKE.md).
