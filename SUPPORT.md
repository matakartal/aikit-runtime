# Support

Use the channel that matches the request:

- Usage questions and design discussions: [GitHub Discussions](https://github.com/matakartal/aikit-runtime/discussions)
- Reproducible defects: [Bug report](https://github.com/matakartal/aikit-runtime/issues/new?template=bug.yml)
- Feature proposals: [Feature request](https://github.com/matakartal/aikit-runtime/issues/new?template=feature.yml)
- Documentation gaps: [Documentation report](https://github.com/matakartal/aikit-runtime/issues/new?template=docs.yml)
- Vulnerabilities: [Private security advisory](https://github.com/matakartal/aikit-runtime/security/advisories/new)

Never post API keys, private prompts, customer data, or vulnerability details in public issues or discussions.

This repository is a source-first `v0.2.0` preview. npm, PyPI, and crates.io installation support
is not offered because no public registry release is claimed. Start with the root
[`README`](README.md), then use the surface-specific guides under `crates/`.

For a reproducible bug report, include the commit SHA, operating system/architecture, Rust/Python/
Node version as relevant, the smallest keyless `mock-1` reproduction, expected result, actual
typed error code, and the validation command you ran. Redact provider output and metadata unless
they are both necessary and safe to disclose.
