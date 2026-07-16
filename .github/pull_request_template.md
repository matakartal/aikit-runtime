## Outcome

<!-- What changes for the user or agent? One or two concrete sentences. -->

## Safety and fidelity

<!-- Permissions, containment, provider reasoning, retries, budgets, audit, compatibility. -->
<!-- Write "N/A" only for pure docs/chore with no runtime impact. -->

## Languages

- [ ] Rust core / facade
- [ ] Python binding
- [ ] Node.js / TypeScript binding
- [ ] Docs only
- [ ] Scripts / CI only

## Verification

- [ ] `cargo fmt --all --check`
- [ ] strict Clippy passed (`-D warnings`)
- [ ] relevant Rust tests passed
- [ ] Python/Node parity checked when a binding/public schema changed
- [ ] docs updated when behavior or limits changed
- [ ] no credentials, private prompts, or generated native artifacts were committed
- [ ] skipped checks and external prerequisites are stated below

## Notes

<!-- Breaking changes, package impact, live smoke status, or follow-up work. -->
