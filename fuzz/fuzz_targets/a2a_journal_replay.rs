#![no_main]

use aikit_core::{A2aMapper, A2aMapperCheckpoint, A2aMapperDelta};
use libfuzzer_sys::fuzz_target;

// Journal rows are read back from disk, so their parsing and replay must fail closed on
// arbitrary bytes: delta validation, checkpoint decoding, and applying a delta to a mapper may
// reject, but must never panic or accept an invariant-violating state.
fuzz_target!(|data: &[u8]| {
    if data.len() > 256 * 1024 {
        return;
    }
    let Some((selector, rest)) = data.split_first() else {
        return;
    };
    match selector % 3 {
        0 => {
            let Ok(delta) = serde_json::from_slice::<A2aMapperDelta>(rest) else {
                return;
            };
            let _ = delta.validate();
            let _ = delta.token();
        }
        1 => {
            let Ok(checkpoint) = serde_json::from_slice::<A2aMapperCheckpoint>(rest) else {
                return;
            };
            let _ = checkpoint.validate();
            let _ = checkpoint.decode_mapper();
        }
        _ => {
            let Ok((mapper, delta)) = serde_json::from_slice::<(A2aMapper, A2aMapperDelta)>(rest)
            else {
                return;
            };
            let _ = mapper.apply_journal_delta(&delta);
        }
    }
});
