//! Emit the Rust-schema field inventory used by `scripts/check-binding-declarations.sh`.
//!
//! For each plain-serde DTO that has a hand-written TypedDict/interface counterpart in the
//! Python/TypeScript declarations, this prints `{type_name: {"fields": [...], "required": [...]}}`
//! derived from the actual Rust types via `schemars`. The declaration gate compares those field
//! sets against the checked-in `.pyi`/`.d.ts` blocks, so a renamed or added Rust field fails CI
//! until the declarations follow. This is field-level drift enforcement, not full generation.

use schemars::{schema_for, JsonSchema};
use serde_json::{json, Map, Value};

fn field_inventory<T: JsonSchema>() -> Value {
    let schema = serde_json::to_value(schema_for!(T)).expect("schema serializes");
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut fields: Vec<String> = properties.keys().cloned().collect();
    fields.sort();
    let declared_required: Vec<String> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    // Same rule the generator applies: a `#[serde(default)]` field without `skip_serializing_if`
    // is always written, so a host always sees it even though parsing tolerates its absence.
    let mut required: Vec<String> = properties
        .iter()
        .filter(|(name, property)| {
            declared_required.contains(name) || property.get("default").is_some()
        })
        .map(|(name, _)| name.clone())
        .collect();
    required.sort();
    json!({"fields": fields, "required": required})
}

fn main() {
    let mut inventory = Map::new();
    inventory.insert(
        "ExternalRuntimeCandidate".into(),
        field_inventory::<aikit_core::RuntimeCandidate>(),
    );
    inventory.insert(
        "ExternalRuntimeArtifact".into(),
        field_inventory::<aikit_core::RuntimeArtifact>(),
    );
    inventory.insert(
        "ExternalRuntimeRelease".into(),
        field_inventory::<aikit_core::RuntimeRelease>(),
    );
    inventory.insert(
        "ExternalUpdateManifest".into(),
        field_inventory::<aikit_core::RuntimeUpdateManifest>(),
    );
    inventory.insert(
        "ExternalUpdateSelection".into(),
        field_inventory::<aikit_core::RuntimeUpdateSelection>(),
    );
    inventory.insert(
        "ExternalUpdateSource".into(),
        field_inventory::<aikit_core::RuntimeUpdateSource>(),
    );
    inventory.insert(
        "ExternalUpdateConfig".into(),
        field_inventory::<aikit_core::RuntimeUpdateConfig>(),
    );
    inventory.insert(
        "ExternalUpdateStateSnapshot".into(),
        field_inventory::<aikit_core::RuntimeUpdateState>(),
    );
    println!(
        "{}",
        serde_json::to_string(&Value::Object(inventory)).expect("inventory serializes")
    );
}
