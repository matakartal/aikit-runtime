// napi-build wires the macOS/Linux linker so the `napi_*` symbols provided by the Node process
// resolve at load time (`-undefined dynamic_lookup` on macOS). This is what lets a plain
// `cargo build -p aikit-node` produce a cdylib that Node can `require()` as a `.node` addon.
fn main() {
    napi_build::setup();
}
