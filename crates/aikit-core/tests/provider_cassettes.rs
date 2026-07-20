use aikit_core::provider_validation::{
    load_and_validate_directory, load_cassette, validate_cassette, CassetteScenario,
};
use std::path::PathBuf;

fn cassette_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("cassettes/providers/v1")
}

#[test]
fn all_eight_provider_cassettes_are_complete_sanitized_and_valid() {
    let report = load_and_validate_directory(cassette_root()).unwrap();
    assert_eq!(report.cassette_count, 8);
    assert_eq!(report.providers.len(), 8);
    assert_eq!(
        report.interaction_count,
        8 * CassetteScenario::REQUIRED.len()
    );
}

#[test]
fn every_provider_has_offline_success_error_and_unsupported_evidence() {
    for entry in std::fs::read_dir(cassette_root()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let cassette = load_cassette(&path).unwrap();
        validate_cassette(&cassette).unwrap();

        let scenarios: Vec<_> = cassette
            .interactions
            .iter()
            .map(|interaction| interaction.scenario)
            .collect();
        for required in CassetteScenario::REQUIRED {
            assert!(
                scenarios.contains(&required),
                "{} misses {required:?}",
                cassette.provider
            );
        }

        let unsupported = cassette
            .interactions
            .iter()
            .find(|item| item.scenario == CassetteScenario::Unsupported)
            .unwrap();
        assert!(!unsupported.network_performed);
        assert_eq!(
            unsupported.typed_error.as_ref().unwrap().code,
            "provider_invalid_request"
        );
    }
}
