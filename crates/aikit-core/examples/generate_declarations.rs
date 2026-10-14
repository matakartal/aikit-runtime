//! Generate the Python and TypeScript declaration blocks for the shared external-agent DTOs
//! directly from the Rust types.
//!
//! `schemars` derives the schema from the same structs `serde` uses on the wire, so field names,
//! optionality, enum variants, and nested references cannot drift from the runtime contract. The
//! emitted blocks live between generated-region markers inside `crates/aikit-py/aikit.pyi` and
//! `crates/aikit-node/index.d.ts`; `scripts/generate-declarations.sh` writes them and its
//! `--check` mode fails CI when the checked-in region differs from a fresh generation.
//!
//! Usage: `cargo run -p aikit-runtime-core --example generate_declarations -- python|typescript`.

use schemars::{schema_for, JsonSchema};
use serde_json::Value;
use std::collections::BTreeMap;

/// One declared type: the Rust schema plus the name the bindings publish it under.
struct DeclaredType {
    declared_name: &'static str,
    schema: Value,
}

fn declared<T: JsonSchema>(declared_name: &'static str) -> DeclaredType {
    DeclaredType {
        declared_name,
        schema: serde_json::to_value(schema_for!(T)).expect("schema serializes"),
    }
}

/// Rust schema title → published declaration name. Every `$ref` must resolve through this map, so
/// a new nested type fails generation loudly instead of emitting an unknown identifier.
fn name_map() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("ExternalAgentKind", "ExternalAgentKind"),
        ("RuntimeProfile", "ExternalRuntimeProfile"),
        ("RuntimeUpdatePolicy", "ExternalUpdatePolicy"),
        ("RuntimeUpdateChannel", "ExternalUpdateChannel"),
        ("RuntimeUpdatePhase", "ExternalUpdatePhase"),
        ("RuntimeRefreshAction", "ExternalRefreshAction"),
        ("RuntimeInstallOrigin", "ExternalRuntimeInstallOrigin"),
        ("RuntimeDiscoverySource", "ExternalRuntimeDiscoverySource"),
        ("RuntimeCandidate", "ExternalRuntimeCandidate"),
        ("RuntimeArtifact", "ExternalRuntimeArtifact"),
        ("RuntimeRelease", "ExternalRuntimeRelease"),
        ("RuntimeUpdateManifest", "ExternalUpdateManifest"),
        ("RuntimeUpdateSelection", "ExternalUpdateSelection"),
        ("RuntimeUpdateSource", "ExternalUpdateSource"),
        ("RuntimeUpdateConfig", "ExternalUpdateConfig"),
        ("RuntimeUpdateState", "ExternalUpdateStateSnapshot"),
        ("Usage", "Usage"),
        ("ProviderWarning", "ProviderWarning"),
        ("MediaInput", "MediaInput"),
        ("MediaInputSource", "MediaInputSource"),
    ])
}

/// Internally tagged enums rendered as discriminated unions. `variant_names` maps the wire tag
/// value to the published per-variant declaration name so existing references stay stable.
struct DeclaredUnion {
    declared_name: &'static str,
    schema: Value,
    variant_names: BTreeMap<&'static str, &'static str>,
}

fn union_types() -> Vec<DeclaredUnion> {
    vec![DeclaredUnion {
        declared_name: "MediaInputSource",
        schema: serde_json::to_value(schema_for!(aikit_core::MediaInputSource))
            .expect("schema serializes"),
        variant_names: BTreeMap::from([
            ("url", "UrlMediaInputSource"),
            ("base64", "Base64MediaInputSource"),
            ("bytes", "BytesMediaInputSource"),
            ("artifact", "ArtifactMediaInputSource"),
        ]),
    }]
}

/// Declaration order is explicit so regenerated output is byte-stable across runs.
fn object_types() -> Vec<DeclaredType> {
    vec![
        declared::<aikit_core::RuntimeCandidate>("ExternalRuntimeCandidate"),
        declared::<aikit_core::RuntimeArtifact>("ExternalRuntimeArtifact"),
        declared::<aikit_core::RuntimeRelease>("ExternalRuntimeRelease"),
        declared::<aikit_core::RuntimeUpdateManifest>("ExternalUpdateManifest"),
        declared::<aikit_core::RuntimeUpdateSelection>("ExternalUpdateSelection"),
        declared::<aikit_core::RuntimeUpdateSource>("ExternalUpdateSource"),
        declared::<aikit_core::RuntimeUpdateConfig>("ExternalUpdateConfig"),
        declared::<aikit_core::RuntimeUpdateState>("ExternalUpdateStateSnapshot"),
        declared::<aikit_core::Usage>("Usage"),
        declared::<aikit_core::ProviderWarning>("ProviderWarning"),
        declared::<aikit_core::MediaInput>("MediaInput"),
    ]
}

fn enum_types() -> Vec<DeclaredType> {
    vec![
        declared::<aikit_core::ExternalAgentKind>("ExternalAgentKind"),
        declared::<aikit_core::RuntimeProfile>("ExternalRuntimeProfile"),
        declared::<aikit_core::RuntimeUpdatePolicy>("ExternalUpdatePolicy"),
        declared::<aikit_core::RuntimeUpdateChannel>("ExternalUpdateChannel"),
        declared::<aikit_core::RuntimeUpdatePhase>("ExternalUpdatePhase"),
        declared::<aikit_core::RuntimeRefreshAction>("ExternalRefreshAction"),
        declared::<aikit_core::RuntimeInstallOrigin>("ExternalRuntimeInstallOrigin"),
        declared::<aikit_core::RuntimeDiscoverySource>("ExternalRuntimeDiscoverySource"),
    ]
}

/// String-enum variants, from either the plain `enum` form or the documented `oneOf`/`const` form.
fn enum_variants(schema: &Value) -> Vec<String> {
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
    }
    schema
        .get("oneOf")
        .and_then(Value::as_array)
        .map(|variants| {
            variants
                .iter()
                .filter_map(|variant| variant.get("const").and_then(Value::as_str))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Strip the nullable half of an `Option<T>` schema so the base type can be rendered.
fn non_null_type(property: &Value) -> Option<&'static str> {
    match property.get("type") {
        Some(Value::String(name)) => scalar_name(name),
        Some(Value::Array(names)) => names
            .iter()
            .filter_map(Value::as_str)
            .find(|name| *name != "null")
            .and_then(scalar_name),
        _ => None,
    }
}

fn scalar_name(name: &str) -> Option<&'static str> {
    match name {
        "string" => Some("string"),
        "integer" => Some("integer"),
        "number" => Some("number"),
        "boolean" => Some("boolean"),
        "array" => Some("array"),
        "object" => Some("object"),
        _ => None,
    }
}

fn ref_name(property: &Value, names: &BTreeMap<&str, &'static str>) -> Option<&'static str> {
    let reference = property.get("$ref").and_then(Value::as_str)?;
    let title = reference.rsplit('/').next()?;
    match names.get(title) {
        Some(declared) => Some(declared),
        None => panic!("unmapped schema reference: {title}"),
    }
}

enum Language {
    Python,
    TypeScript,
}

fn render_property(
    property: &Value,
    names: &BTreeMap<&str, &'static str>,
    language: &Language,
) -> String {
    if let Some(declared) = ref_name(property, names) {
        return declared.to_owned();
    }
    match non_null_type(property) {
        Some("string") => match language {
            Language::Python => "str".into(),
            Language::TypeScript => "string".into(),
        },
        Some("integer") => match language {
            Language::Python => "int".into(),
            Language::TypeScript => "number".into(),
        },
        Some("number") => match language {
            Language::Python => "float".into(),
            Language::TypeScript => "number".into(),
        },
        Some("boolean") => match language {
            Language::Python => "bool".into(),
            Language::TypeScript => "boolean".into(),
        },
        Some("array") => {
            let items = property
                .get("items")
                .expect("array schemas declare their item type");
            let inner = render_property(items, names, language);
            match language {
                Language::Python => format!("List[{inner}]"),
                Language::TypeScript => format!("{inner}[]"),
            }
        }
        Some("object") | None => match language {
            // A free-form map keeps the binding's JSON value type rather than inventing a shape.
            Language::Python => "Dict[str, Any]".into(),
            Language::TypeScript => "Record<string, JsonValue>".into(),
        },
        Some(other) => panic!("unsupported schema type: {other}"),
    }
}

/// Ordered `(name, rendered_type, required)` triples for one object schema.
fn properties(
    schema: &Value,
    names: &BTreeMap<&str, &'static str>,
    language: &Language,
) -> Vec<(String, String, bool)> {
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    schema
        .get("properties")
        .and_then(Value::as_object)
        .expect("object schemas declare properties")
        .iter()
        .map(|(name, property)| {
            // A `#[serde(default)]` field without `skip_serializing_if` is always written, so it
            // is always present for a host reading the value even though parsing tolerates its
            // absence. `skip_serializing_if` fields carry no schema default and stay optional.
            let always_present =
                required.contains(&name.as_str()) || property.get("default").is_some();
            (
                name.clone(),
                render_property(property, names, language),
                always_present,
            )
        })
        .collect()
}

/// `(tag_value, variant_schema)` pairs for an internally tagged enum schema.
fn union_variants(schema: &Value) -> Vec<(String, Value)> {
    schema
        .get("oneOf")
        .and_then(Value::as_array)
        .expect("tagged unions serialize as oneOf")
        .iter()
        .map(|variant| {
            let properties = variant
                .get("properties")
                .and_then(Value::as_object)
                .expect("tagged variants are objects");
            let tag = properties
                .values()
                .find_map(|property| property.get("const").and_then(Value::as_str))
                .expect("tagged variants carry a const discriminant")
                .to_owned();
            (tag, variant.clone())
        })
        .collect()
}

fn variant_declared_name<'a>(union: &'a DeclaredUnion, tag: &str) -> &'a str {
    match union.variant_names.get(tag) {
        Some(name) => name,
        None => panic!("unmapped union variant tag: {tag}"),
    }
}

fn render_python(names: &BTreeMap<&str, &'static str>) -> String {
    let mut out = String::new();
    for declared in enum_types() {
        let variants = enum_variants(&declared.schema)
            .into_iter()
            .map(|variant| format!("\"{variant}\""))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "{} = Literal[{variants}]\n",
            declared.declared_name
        ));
    }
    for union in union_types() {
        let variants = union_variants(&union.schema);
        for (tag, variant) in &variants {
            let name = variant_declared_name(&union, tag);
            out.push('\n');
            out.push('\n');
            out.push_str(&format!("class {name}(TypedDict):\n"));
            for (field, rendered, _) in properties(variant, names, &Language::Python) {
                let rendered = if variant
                    .get("properties")
                    .and_then(|properties| properties.get(&field))
                    .and_then(|property| property.get("const"))
                    .is_some()
                {
                    format!("Literal[\"{tag}\"]")
                } else {
                    rendered
                };
                out.push_str(&format!("    {field}: {rendered}\n"));
            }
        }
        out.push('\n');
        out.push('\n');
        out.push_str(&format!("{} = Union[\n", union.declared_name));
        for (tag, _) in &variants {
            out.push_str(&format!("    {},\n", variant_declared_name(&union, tag)));
        }
        out.push_str("]\n");
    }
    for declared in object_types() {
        let fields = properties(&declared.schema, names, &Language::Python);
        let required: Vec<_> = fields.iter().filter(|(_, _, req)| *req).collect();
        let optional: Vec<_> = fields.iter().filter(|(_, _, req)| !*req).collect();
        out.push('\n');
        out.push('\n');
        match (required.is_empty(), optional.is_empty()) {
            (false, true) => {
                out.push_str(&format!("class {}(TypedDict):\n", declared.declared_name));
                for (name, rendered, _) in &required {
                    out.push_str(&format!("    {name}: {rendered}\n"));
                }
            }
            (true, _) => {
                out.push_str(&format!(
                    "class {}(TypedDict, total=False):\n",
                    declared.declared_name
                ));
                for (name, rendered, _) in &optional {
                    out.push_str(&format!("    {name}: {rendered}\n"));
                }
            }
            (false, false) => {
                out.push_str(&format!(
                    "class _{}Required(TypedDict):\n",
                    declared.declared_name
                ));
                for (name, rendered, _) in &required {
                    out.push_str(&format!("    {name}: {rendered}\n"));
                }
                out.push('\n');
                out.push('\n');
                out.push_str(&format!(
                    "class {}(_{}Required, total=False):\n",
                    declared.declared_name, declared.declared_name
                ));
                for (name, rendered, _) in &optional {
                    out.push_str(&format!("    {name}: {rendered}\n"));
                }
            }
        }
    }
    out
}

fn render_typescript(names: &BTreeMap<&str, &'static str>) -> String {
    let mut out = String::new();
    for declared in enum_types() {
        let variants = enum_variants(&declared.schema)
            .into_iter()
            .map(|variant| format!("\"{variant}\""))
            .collect::<Vec<_>>()
            .join(" | ");
        out.push_str(&format!(
            "export type {} = {variants};\n",
            declared.declared_name
        ));
    }
    for union in union_types() {
        let variants = union_variants(&union.schema);
        out.push('\n');
        out.push_str(&format!("export type {} =\n", union.declared_name));
        for (index, (tag, variant)) in variants.iter().enumerate() {
            let fields = properties(variant, names, &Language::TypeScript)
                .into_iter()
                .map(|(field, rendered, _)| {
                    let rendered = if variant
                        .get("properties")
                        .and_then(|properties| properties.get(&field))
                        .and_then(|property| property.get("const"))
                        .is_some()
                    {
                        format!("\"{tag}\"")
                    } else {
                        rendered
                    };
                    format!("{field}: {rendered}")
                })
                .collect::<Vec<_>>()
                .join("; ");
            let terminator = if index + 1 == variants.len() { ";" } else { "" };
            out.push_str(&format!("  | {{ {fields} }}{terminator}\n"));
        }
    }
    for declared in object_types() {
        out.push('\n');
        out.push_str(&format!("export interface {} {{\n", declared.declared_name));
        for (name, rendered, required) in properties(&declared.schema, names, &Language::TypeScript)
        {
            let optional = if required { "" } else { "?" };
            out.push_str(&format!("  {name}{optional}: {rendered};\n"));
        }
        out.push_str("}\n");
    }
    out
}

fn main() {
    let names = name_map();
    let language = std::env::args().nth(1).unwrap_or_default();
    let rendered = match language.as_str() {
        "python" => render_python(&names),
        "typescript" => render_typescript(&names),
        other => {
            eprintln!("usage: generate_declarations python|typescript (got {other:?})");
            std::process::exit(2);
        }
    };
    print!("{rendered}");
}
