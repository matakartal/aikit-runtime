#![no_main]

use aikit_core::{RunEvent, RunState};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(events) = serde_json::from_slice::<Vec<RunEvent>>(data) else {
        return;
    };
    if events.len() <= 256 {
        let _ = RunState::from_events(events);
    }
});
