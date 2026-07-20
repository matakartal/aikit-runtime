#![no_main]

use aikit_core::provider_validation::{validate_cassette, ProviderCassette};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(cassette) = serde_json::from_slice::<ProviderCassette>(data) else {
        return;
    };
    if cassette.interactions.len() <= 64 {
        let _ = validate_cassette(&cassette);
    }
});
