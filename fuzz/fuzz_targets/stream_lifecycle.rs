#![no_main]

use aikit_core::{StreamDelta, StreamEventEncoder};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(deltas) = serde_json::from_slice::<Vec<StreamDelta>>(data) else {
        return;
    };
    let mut encoder = StreamEventEncoder::new("fuzz-response");
    for delta in deltas.into_iter().take(256) {
        let _ = encoder.try_push(delta);
    }
});
